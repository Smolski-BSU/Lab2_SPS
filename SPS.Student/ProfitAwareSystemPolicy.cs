using SPS.Abstractions;

namespace SPS.Student;

/// <summary>
/// Стратегия для лабораторной №2 (Service Profit Simulator).
/// Цель: максимальный Profit = выручка за дождавшихся клиентов - стоимость API и воркеров.
///
/// Основные идеи:
///  1. Бюджет обработки = верхняя граница терпения типа запроса. Клиент, который точно ушёл,
///     больше не тратит ресурсы: его работа выкидывается из очередей (особенно важно для Generate).
///  2. Кэш. Запись выдаётся, только если она будет достаточно свежей для КОНКРЕТНОГО клиента.
///     Иначе клиент присоединяется к уже идущей загрузке ключа (база - общее узкое место),
///     и только если загрузки нет - начинается новая. TTL = максимальное требование свежести
///     для типа; при нехватке памяти вытесняем бесполезные, потом наименее популярные на байт.
///  3. Задания Generate. Ценность задания = цена * P(клиент ещё ждёт к моменту готовности)
///     - стоимость воркера на время работы. Назначаем задание с наибольшей ценностью на секунду
///     работы самому быстрому свободному воркеру; безнадёжные отклоняем.
///  4. Мощности. Задания могут бесплатно ждать ~15 с (минимальное терпение), поэтому воркеров
///     держим столько, сколько нужно, чтобы успеть выполнить очередь к этим срокам (EDF-оценка),
///     но не меньше средней нагрузки. Незанятые воркеры сразу выводим. API нагружен слабо - держим
///     один, добавляем при накоплении вызовов, в хвосте прогона (нет запросов) выключаем.
///  5. Здоровье. API блокируем после серии тайм-аутов и возвращаем после успешной пробы.
///     Воркер без heartbeat проверяем пробой: Success/Rejected = жив (потерялись только heartbeat),
///     тайм-аут = мёртв -> его задания переназначаем, поднимаем замену, мёртвый выводим.
///     Задание, о котором живой воркер молчит в heartbeat (назначение потерялось в сети или публикация
///     результата отброшена переполненной базой), считается потерянным и переназначается.
///  6. RPC-политика (развитие AdaptiveRequestPolicy из лабы №1): тайм-аут по оценке задержки
///     (srtt + k * rttvar, отдельно для каждого типа запроса), ограниченный оставшимся терпением;
///     повторы - пока клиент может дождаться, с ограничением числа попыток и бюджетом повторов.
/// </summary>
public sealed class ProfitAwareSystemPolicy : ISystemPolicy
{
    // ---------------- Параметры (подобраны на нескольких сценариях и 20 seed) ----------------
    private const double DeliveryDelay = 0.002;       // доставка из кэша/загрузки клиенту
    private const double PopularityHalfLife = 8.0;    // с, затухание популярности ключа
    private const double GenRateWindow = 5.0;         // с, окно оценки потока Generate
    private const double JobOverhead = 0.2;           // с, сеть + публикация результата в базе
    private const double FreeWaitSeconds = 15.0;      // срок для EDF-оценки числа воркеров
    private const double LoadFactor = 0.8;            // доля средней нагрузки, которую держим всегда
    private const double RoundDown = 0.45;            // не поднимать воркер ради малой дробной части
    private const double DeadRetireDelay = 2.5;       // с, через сколько выводить мёртвый экземпляр
    private const double TailMinGap = 2.0;            // с без запросов -> API можно выключить
    private const double TimeoutScale = 6.0;          // множитель нижней границы тайм-аута RPC
    private const double MaxTimeoutSeconds = 10.0;    // верхняя граница тайм-аута RPC (в лабе №1 было 1 с)
    private const int ApiFailuresToBlock = 3;         // неудач подряд, после которых API блокируется
    private const double RttVarWeight = 8.0;          // тайм-аут = srtt + k * rttvar

    // ---------------- Терпение клиентов (общие диапазоны из условия) ----------------
    private static double PatienceMin(RequestKind k) => k switch
    {
        RequestKind.Metadata => 0.5,
        RequestKind.Preview => 1.0,
        _ => 15.0
    };
    private static double PatienceMax(RequestKind k) => k switch
    {
        RequestKind.Metadata => 1.5,
        RequestKind.Preview => 3.0,
        _ => 60.0
    };

    // Вероятность, что клиент ещё ждёт через t секунд после прихода (терпение ~ равномерное).
    private static double Survival(RequestKind k, double t)
    {
        double lo = PatienceMin(k), hi = PatienceMax(k);
        if (t <= lo) return 1.0;
        if (t >= hi) return 0.0;
        return (hi - t) / (hi - lo);
    }

    // ---------------- Состояние ----------------
    private SystemSettings? _settings;
    private TimeSpan _now;                       // последнее известное модельное время
    private RequestInfo? _pendingRpcRequest;     // запрос, для которого движок сейчас спросит тайм-аут

    private readonly Dictionary<string, (double Score, double At)> _popularity = new(StringComparer.Ordinal);
    private readonly double[] _maxAgeSeen = new double[3];

    private double _genRate, _genRateAt;         // поток Generate, заданий/с (EWMA)
    private double _genWork = 5.0;               // средняя оценка работы задания, с
    private bool _genWorkInit;

    private double _arrivalRate, _lastArrival;   // общий поток запросов
    private double _lastScaleDownAt;

    private sealed class InstanceStatus
    {
        public bool Dead;
        public TimeSpan DeadSince;
        public bool Suspect;
        public TimeSpan SuspectSince;
        public TimeSpan ReadyAt;
        public TimeSpan? LastHeartbeat;
        public double LatencyEwma = -1;
    }
    private readonly Dictionary<long, InstanceStatus> _inst = new();
    private readonly HashSet<long> _stopped = new();

    public IRequestPolicy RpcPolicy { get; }

    public ProfitAwareSystemPolicy()
    {
        RpcPolicy = new AdaptiveRpcPolicy(this);
    }

    private InstanceStatus Get(long id)
    {
        if (!_inst.TryGetValue(id, out var s)) { s = new InstanceStatus(); _inst[id] = s; }
        return s;
    }

    private double WorkerCostPerSecond => (double)(_settings?.Worker.PricePerSecond ?? 0.30m);
    private double HeartbeatInterval => _settings?.HeartbeatInterval.TotalSeconds ?? 0.5;

    // =====================================================================
    // Бюджет обработки (вызывается один раз при поступлении любого запроса)
    // =====================================================================
    public TimeSpan? GetProcessingBudget(RequestInfo request, Random random)
    {
        _now = request.CreatedAt;
        double now = request.CreatedAt.TotalSeconds;
        int k = (int)request.Kind;
        if (request.MaxDataAge.TotalSeconds > _maxAgeSeen[k]) _maxAgeSeen[k] = request.MaxDataAge.TotalSeconds;

        if (request.Kind == RequestKind.Generate)
        {
            _genRate = _genRate * Math.Exp(-Math.Max(0, now - _genRateAt) / GenRateWindow) + 1.0 / GenRateWindow;
            _genRateAt = now;
            if (!_genWorkInit) { _genWork = request.EstimatedWorkSeconds; _genWorkInit = true; }
            else _genWork = 0.97 * _genWork + 0.03 * request.EstimatedWorkSeconds;
        }
        else Touch(request.ResourceKey, now);

        // Дальше максимального терпения клиент точно не ждёт - работа после этого бесполезна.
        return TimeSpan.FromSeconds(PatienceMax(request.Kind));
    }

    private double CurrentGenRate(double now) => _genRate * Math.Exp(-Math.Max(0, now - _genRateAt) / GenRateWindow);

    private void Touch(string key, double now)
    {
        _popularity[key] = _popularity.TryGetValue(key, out var p)
            ? (p.Score * Math.Pow(0.5, (now - p.At) / PopularityHalfLife) + 1, now)
            : (1, now);
    }

    private double Popularity(string key, double now) =>
        _popularity.TryGetValue(key, out var p) ? p.Score * Math.Pow(0.5, (now - p.At) / PopularityHalfLife) : 0;

    // =====================================================================
    // Кэш
    // =====================================================================
    public CacheReadDecision OnCacheRead(CacheReadContext context, Random random)
    {
        _now = context.Now;
        var req = context.Request;
        double now = context.Now.TotalSeconds;

        // 1) Запись из кэша - если к моменту доставки она будет достаточно свежей для этого клиента.
        var entry = context.Entry;
        if (entry != null && now + DeliveryDelay + 0.001 - entry.SourceObservedAt.TotalSeconds <= req.MaxDataAge.TotalSeconds)
            return CacheReadDecision.ServeCached();

        // 2) Присоединяемся к самой свежей идущей загрузке: результат будет прочитан после её старта,
        //    значит свежий, а база не получит лишней работы.
        FetchInfo? best = null;
        foreach (var f in context.PendingFetches)
            if (now - f.StartedAt.TotalSeconds <= PatienceMax(req.Kind) && (best == null || f.StartedAt > best.StartedAt))
                best = f;
        if (best != null) return CacheReadDecision.JoinFetch(best.Id);

        // 3) Новая загрузка. Для Metadata движок сразу спросит тайм-аут RPC - запоминаем запрос.
        if (req.Kind == RequestKind.Metadata) _pendingRpcRequest = req;
        return CacheReadDecision.StartFetch();
    }

    public CacheWriteDecision OnCacheFill(CacheFillContext context, Random random)
    {
        _now = context.Now;
        double now = context.Now.TotalSeconds;
        var v = context.Value;
        double maxAge = MaxAge(KindOfKey(v.Key));
        // Запись полезна, пока она моложе самого мягкого требования свежести для этого типа.
        var ttl = TimeSpan.FromSeconds(Math.Max(0.05, maxAge - (now - v.SourceObservedAt.TotalSeconds)));

        long used = 0;
        foreach (var e in context.Entries) if (e.Key != v.Key) used += e.SizeBytes;
        long free = context.CapacityBytes - used;
        if (v.SizeBytes <= free) return CacheWriteDecision.Retain(ttl);
        if (v.SizeBytes > context.CapacityBytes) return CacheWriteDecision.Skip();

        // Вытеснение: сначала уже бесполезные (слишком старые), потом наименее популярные на байт,
        // но только если новая запись ценнее вытесняемых.
        double newScore = Popularity(v.Key, now) / v.SizeBytes;
        var candidates = context.Entries.Where(e => e.Key != v.Key)
            .Select(e => (Entry: e,
                Useless: now - e.SourceObservedAt.TotalSeconds >= MaxAge(KindOfKey(e.Key)) - 0.05,
                Score: Popularity(e.Key, now) / e.SizeBytes))
            .OrderByDescending(c => c.Useless).ThenBy(c => c.Score).ToList();
        var evict = new List<string>();
        foreach (var c in candidates)
        {
            if (free >= v.SizeBytes) break;
            if (!c.Useless && c.Score >= newScore) break;
            evict.Add(c.Entry.Key);
            free += c.Entry.SizeBytes;
        }
        if (free < v.SizeBytes) return CacheWriteDecision.Skip();
        return CacheWriteDecision.Retain(ttl, evict.ToArray());
    }

    private static RequestKind KindOfKey(string key) =>
        key.StartsWith("Preview", StringComparison.Ordinal) ? RequestKind.Preview :
        key.StartsWith("Metadata", StringComparison.Ordinal) ? RequestKind.Metadata : RequestKind.Generate;

    private double MaxAge(RequestKind k) =>
        _maxAgeSeen[(int)k] > 0 ? _maxAgeSeen[(int)k] : (k == RequestKind.Metadata ? 2 : 5);

    // =====================================================================
    // Маршрутизация на API (Preview и регистрация Generate)
    // =====================================================================
    public EndpointSelectionDecision? SelectEndpoint(RoutingContext context, Random random)
    {
        _now = context.Now;
        var req = context.Request;
        if ((context.Now - req.CreatedAt).TotalSeconds >= PatienceMax(req.Kind)) return EndpointSelectionDecision.Reject();
        if (context.Endpoints.Count == 0) return null; // подождём, пока появится здоровый API

        // Меньшая наблюдаемая задержка и загрузка (с учётом скорости экземпляра).
        EndpointInfo? best = null;
        double bestScore = double.MaxValue;
        foreach (var e in context.Endpoints)
        {
            if (e.State != InstanceState.Ready) continue;
            var st = Get(e.Id);
            double load = (e.OutstandingCalls + 1.0) / (e.Slots * e.Speed);
            double latency = st.LatencyEwma > 0 ? st.LatencyEwma : 0.05;
            double score = load * 0.02 + latency;
            if (score < bestScore) { bestScore = score; best = e; }
        }
        if (best == null) return null;
        _pendingRpcRequest = req; // сразу после назначения движок спросит тайм-аут
        return EndpointSelectionDecision.Assign(best.Id);
    }

    // =====================================================================
    // Здоровье экземпляров
    // =====================================================================
    public EndpointDecision ReviewEndpoint(EndpointContext context, Random random)
    {
        _now = context.Now;
        var e = context.Endpoint;
        var st = Get(e.Id);
        double now = context.Now.TotalSeconds;
        if (e.Role == InstanceRole.Worker) return ReviewWorker(context, st, now);

        var results = context.RecentResults;
        if (!st.Dead)
        {
            // Три подряд неудачи (тайм-аут/недоступен) за последние 2 с -> блокируем и пробуем.
            // Rejected означает "жив, но перегружен" и не считается.
            int fails = 0, considered = 0;
            for (int i = results.Count - 1; i >= 0 && considered < 4; i--)
            {
                var r = results[i];
                if (now - r.Time.TotalSeconds > 2.0) break;
                if (r.IsProbe) continue;
                considered++;
                if (r.Outcome is RequestOutcome.Timeout or RequestOutcome.Unavailable) fails++;
                else break;
            }
            if (fails >= ApiFailuresToBlock)
            {
                st.Dead = true;
                st.DeadSince = context.Now;
                return EndpointDecision.Probe();
            }
            return EndpointDecision.Allow();
        }

        // Заблокирован: возвращаем трафик после быстрой успешной пробы, отправленной после блокировки.
        for (int i = results.Count - 1; i >= 0; i--)
        {
            var r = results[i];
            if (r.Time <= st.DeadSince) break;
            if (!r.IsProbe) continue;
            if (r.Outcome == RequestOutcome.Success && r.Elapsed.TotalSeconds < 0.15)
            {
                st.Dead = false;
                st.LatencyEwma = -1;
                return EndpointDecision.Allow();
            }
            break;
        }
        return EndpointDecision.Probe();
    }

    private EndpointDecision ReviewWorker(EndpointContext context, InstanceStatus st, double now)
    {
        var e = context.Endpoint;
        if (st.ReadyAt == TimeSpan.Zero && e.State == InstanceState.Ready) st.ReadyAt = context.Now;
        st.LastHeartbeat = e.LastHeartbeat;
        double hbAge = now - (e.LastHeartbeat ?? st.ReadyAt).TotalSeconds;
        if (hbAge <= HeartbeatInterval * 2.2 + 0.05)
        {
            st.Suspect = false;
            st.Dead = false;
            return EndpointDecision.Allow();
        }

        // Heartbeat пропал. Проба отвечает Success (свободен) или Rejected (занят) - значит воркер жив
        // и потерялись только heartbeat'ы; тайм-аут пробы - воркер мёртв или недоступен.
        if (!st.Suspect) { st.Suspect = true; st.SuspectSince = context.Now; return EndpointDecision.Probe(); }
        var results = context.RecentResults;
        for (int i = results.Count - 1; i >= 0; i--)
        {
            var r = results[i];
            if (r.Time <= st.SuspectSince) break;
            if (!r.IsProbe) continue;
            if (r.Outcome is RequestOutcome.Success or RequestOutcome.Rejected)
            {
                st.Dead = false;
                st.SuspectSince = context.Now;
                return EndpointDecision.Allow();
            }
            if (r.Outcome == RequestOutcome.Timeout && !st.Dead) { st.Dead = true; st.DeadSince = context.Now; }
            break;
        }
        return EndpointDecision.Probe();
    }

    // =====================================================================
    // Задания Generate
    // =====================================================================
    private double JobValue(JobInfo job, double now, double speed, out double runSeconds)
    {
        var r = job.Request;
        runSeconds = r.EstimatedWorkSeconds / Math.Max(0.1, speed) + JobOverhead;
        double finishAge = now - r.CreatedAt.TotalSeconds + runSeconds;
        return (double)r.Revenue * Survival(RequestKind.Generate, finishAge) - WorkerCostPerSecond * runSeconds;
    }

    public JobAssignmentDecision? SelectJobAssignment(JobSchedulingContext context, Random random)
    {
        _now = context.Now;
        double now = context.Now.TotalSeconds;
        if (context.QueuedJobs.Count == 0 || context.AvailableWorkers.Count == 0) return null;

        // Самый быстрый свободный и здоровый воркер.
        EndpointInfo? worker = null;
        foreach (var w in context.AvailableWorkers)
        {
            if (Get(w.Id).Dead) continue;
            if (worker == null || w.Speed > worker.Speed) worker = w;
        }
        if (worker == null) return null;

        // Задание с наибольшей ожидаемой прибылью на секунду работы воркера.
        JobInfo? bestJob = null;
        double bestPriority = double.MinValue;
        foreach (var job in context.QueuedJobs)
        {
            double value = JobValue(job, now, worker.Speed, out double run);
            if (value <= 0)
            {
                // Клиент гарантированно не дождётся - сразу закрываем запрос.
                if (now - job.Request.CreatedAt.TotalSeconds + run >= PatienceMax(RequestKind.Generate))
                    return JobAssignmentDecision.Reject(job.Id);
                continue; // невыгодно сейчас, но может пригодиться более быстрому воркеру
            }
            double priority = value / run;
            if (priority > bestPriority) { bestPriority = priority; bestJob = job; }
        }
        return bestJob == null ? null : JobAssignmentDecision.Assign(bestJob.Id, worker.Id);
    }

    public JobRecoveryDecision ReviewJob(JobRecoveryContext context, Random random)
    {
        _now = context.Now;
        var job = context.Job;
        if (job.WorkerId is not long wid || job.AssignedAt is not TimeSpan assignedAt) return JobRecoveryDecision.KeepWaiting();

        // Воркер остановлен или признан мёртвым - результата не будет.
        if (_stopped.Contains(wid) || Get(wid).Dead) return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);

        // Воркер шлёт heartbeat'ы, а про это задание молчит - значит, задание потеряно.
        var st = Get(wid);
        if (st.LastHeartbeat is TimeSpan whb)
        {
            // а) Назначение не дошло до воркера (heartbeat про задание не было ни разу).
            double sinceAssign = (context.Now - assignedAt).TotalSeconds;
            if (job.LastHeartbeat == null && whb.TotalSeconds > assignedAt.TotalSeconds + HeartbeatInterval + 0.15 &&
                sinceAssign > HeartbeatInterval * 2)
                return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
            // б) Задание выполнялось, но пропало из heartbeat'ов, а результата нет. Так бывает, когда очередь базы
            //    переполнена и публикация результата отброшена: без переназначения воркер простаивал бы
            //    занятым до конца бюджета клиента (до 60 с). Пока задание на воркере (в том числе ждёт базу),
            //    каждый доставленный heartbeat обновляет и время задания, поэтому ложных срабатываний нет.
            if (job.LastHeartbeat is TimeSpan jhb && whb.TotalSeconds > jhb.TotalSeconds + HeartbeatInterval * 0.5)
                return JobRecoveryDecision.RequeueAfter(TimeSpan.Zero);
        }

        // Иначе ждём: раннее переназначение создаёт дублирующую работу.
        return JobRecoveryDecision.KeepWaiting();
    }

    // =====================================================================
    // Мощности
    // =====================================================================
    public CapacityPlan ReviewCapacity(CapacityContext context, Random random)
    {
        _now = context.Now;
        _settings ??= context.Settings;
        var s = context.Settings;
        double now = context.Now.TotalSeconds;

        var apis = context.Instances.Where(i => i.Role == InstanceRole.Api && i.State is InstanceState.Ready or InstanceState.Starting).ToList();
        var workers = context.Instances.Where(i => i.Role == InstanceRole.Worker && i.State is InstanceState.Ready or InstanceState.Starting).ToList();
        var retire = new List<long>();

        // ---------- Воркеры ----------
        // Средняя нагрузка: поток заданий * время на задание.
        double speed = s.Worker.Speed;
        double perJob = _genWork / speed + JobOverhead;
        double load = CurrentGenRate(now) * perJob;

        // EDF-оценка: сколько воркеров нужно, чтобы выполнить все выгодные задания очереди
        // к сроку "приход + 15 с" (до этого срока ожидание ничего не стоит).
        var queue = new List<(double Due, double Work)>();
        foreach (var j in context.QueuedJobs)
        {
            double run = j.Request.EstimatedWorkSeconds / speed + JobOverhead;
            double age = now - j.Request.CreatedAt.TotalSeconds;
            if ((double)j.Request.Revenue * Survival(RequestKind.Generate, age + run) - WorkerCostPerSecond * run <= 0) continue;
            queue.Add((j.Request.CreatedAt.TotalSeconds + FreeWaitSeconds, run));
        }
        queue.Sort((a, b) => a.Due.CompareTo(b.Due));
        double cumulative = workers.Count(w => w.AssignedJobs > 0) * perJob * 0.5; // остаток текущих работ
        double required = 0;
        foreach (var (due, work) in queue)
        {
            cumulative += work;
            required = Math.Max(required, cumulative / Math.Max(1.0, due - now));
        }
        int desiredWorkers = Math.Max(0, (int)Math.Ceiling(Math.Max(required, load * LoadFactor) - RoundDown));

        // ---------- API ----------
        int desiredApis = 1;
        int readyApis = Math.Max(1, apis.Count(a => a.State == InstanceState.Ready && !Get(a.Id).Dead));
        if (context.PendingCalls > readyApis * s.Api.Slots * 2) desiredApis = readyApis + 1;
        // Хвост прогона: давно нет запросов и нет ожидающих вызовов - API не нужен.
        bool idleTail = now - _lastArrival > Math.Max(TailMinGap, 12.0 / Math.Max(0.01, _arrivalRate)) && context.PendingCalls == 0;
        if (idleTail) desiredApis = 0;

        // ---------- Мёртвые экземпляры ----------
        // Сначала поднимаем замену (мёртвый остаётся в счёте, поэтому новый экземпляр получает
        // другой порядковый номер и не попадает под тот же сбой), затем выводим мёртвый.
        var deadWorkers = workers.Where(w => Get(w.Id).Dead).ToList();
        foreach (var d in deadWorkers)
        {
            if (now - Get(d.Id).DeadSince.TotalSeconds > DeadRetireDelay) retire.Add(d.Id);
            else desiredWorkers++;
        }
        foreach (var d in apis.Where(a => Get(a.Id).Dead))
        {
            if (now - Get(d.Id).DeadSince.TotalSeconds > DeadRetireDelay) retire.Add(d.Id);
            else desiredApis++;
        }

        // ---------- Уменьшение воркеров ----------
        // Не чаще раза в секунду и только за счёт незанятых (занятый воркер при выводе может
        // не успеть доделать задание за отведённое время).
        int currentWorkers = workers.Count - deadWorkers.Count(d => retire.Contains(d.Id));
        if (desiredWorkers < currentWorkers)
        {
            if (now - _lastScaleDownAt < 1.0) desiredWorkers = currentWorkers;
            else
            {
                int idle = workers.Count(w => !Get(w.Id).Dead && (w.State == InstanceState.Starting || w.AssignedJobs == 0));
                desiredWorkers = Math.Max(desiredWorkers, currentWorkers - idle);
                _lastScaleDownAt = now;
            }
        }

        // ---------- Лимиты ----------
        desiredApis = Math.Clamp(desiredApis, idleTail ? 0 : 1, s.MaximumApis);
        desiredWorkers = Math.Clamp(desiredWorkers, 0, s.MaximumWorkers);
        while (desiredApis * s.Api.ComputeUnits + desiredWorkers * s.Worker.ComputeUnits > s.ComputeUnitLimit && desiredWorkers > 0)
            desiredWorkers--;
        while (desiredApis * s.Api.ComputeUnits + desiredWorkers * s.Worker.ComputeUnits > s.ComputeUnitLimit && desiredApis > 1)
            desiredApis--;
        return new CapacityPlan(desiredApis, desiredWorkers, retire.Count > 0 ? retire : null);
    }

    // =====================================================================
    // Наблюдения
    // =====================================================================
    public void Observe(SystemObservation observation)
    {
        var o = observation;
        _now = o.Now;
        switch (o.Kind)
        {
            case ObservationKind.SimulationStarted:
                _settings = o.Settings;
                break;
            case ObservationKind.RequestArrived:
                double t = o.Now.TotalSeconds;
                _arrivalRate = _arrivalRate * Math.Exp(-Math.Max(0, t - _lastArrival) / 10.0) + 0.1;
                _lastArrival = t;
                break;
            case ObservationKind.RpcAttemptCompleted:
                if (o.EndpointId is long eid)
                {
                    var st = Get(eid);
                    if (o.Outcome == RequestOutcome.Success && o.Elapsed is TimeSpan el)
                        st.LatencyEwma = st.LatencyEwma < 0 ? el.TotalSeconds : 0.8 * st.LatencyEwma + 0.2 * el.TotalSeconds;
                    else if (o.Outcome == RequestOutcome.Timeout)
                        st.LatencyEwma = st.LatencyEwma < 0 ? 0.3 : 0.8 * st.LatencyEwma + 0.2 * 0.5;
                }
                break;
            case ObservationKind.WorkerHeartbeatReceived:
                if (o.EndpointId is long wid) Get(wid).LastHeartbeat = o.Now;
                break;
            case ObservationKind.InstanceStopped:
                if (o.EndpointId is long sid) { _inst.Remove(sid); _stopped.Add(sid); }
                break;
        }
    }

    // =====================================================================
    // RPC-политика (развитие AdaptiveRequestPolicy из лабы №1)
    // =====================================================================
    // Отличия от первой лабы: оценка задержки ведётся отдельно для каждого типа запроса,
    // тайм-аут и повторы ограничены оставшимся терпением клиента (тип и время прихода
    // известны общей стратегии), на перегрузку (Rejected) отвечаем паузой.
    private sealed class AdaptiveRpcPolicy : IRequestPolicy
    {
        private readonly ProfitAwareSystemPolicy _owner;
        private readonly Dictionary<long, (RequestInfo Req, TimeSpan StartedAt)> _rpcs = new();
        private readonly double[] _srtt = { 0.05, 0.10, 0.05 };
        private readonly double[] _rttvar = { 0.02, 0.04, 0.02 };
        private readonly bool[] _hasSample = new bool[3];
        private double _retryTokens = 20;

        public AdaptiveRpcPolicy(ProfitAwareSystemPolicy owner) { _owner = owner; }

        public TimeSpan GetTimeout(RequestContext context, Random random)
        {
            RequestInfo? req = null;
            TimeSpan now = _owner._now;
            _rpcs.TryGetValue(context.RequestId, out var previous);
            if (_owner._pendingRpcRequest != null)
            {
                req = _owner._pendingRpcRequest;
                _owner._pendingRpcRequest = null;
            }
            else if (previous.Req != null)
            {
                req = previous.Req;
                // Повтор Metadata идёт без обращения к стратегии - время берём из запланированного повтора.
                if (now < previous.StartedAt) now = previous.StartedAt;
            }
            if (req == null) return TimeSpan.FromMilliseconds(250);
            _rpcs[context.RequestId] = (req, now);

            int k = (int)req.Kind;
            double minT = TimeoutScale * (req.Kind switch { RequestKind.Metadata => 0.06, RequestKind.Preview => 0.08, _ => 0.10 });
            double maxT = MaxTimeoutSeconds;
            double t = Math.Clamp((_srtt[k] + RttVarWeight * _rttvar[k]) * Math.Pow(1.3, context.Attempt - 1), minT, maxT);
            double remaining = PatienceMax(req.Kind) - (now - req.CreatedAt).TotalSeconds;
            return TimeSpan.FromSeconds(Math.Min(t, Math.Max(0.02, remaining)));
        }

        public RetryDecision OnResult(RequestObservation observation, Random random)
        {
            if (!_rpcs.TryGetValue(observation.RequestId, out var info)) return RetryDecision.NoRetry();
            var req = info.Req;
            int k = (int)req.Kind;
            var now = info.StartedAt + observation.Elapsed;
            _owner._now = now;

            if (observation.Outcome == RequestOutcome.Success)
            {
                double x = observation.Elapsed.TotalSeconds;
                if (!_hasSample[k]) { _srtt[k] = x; _rttvar[k] = x / 2; _hasSample[k] = true; }
                else
                {
                    _rttvar[k] = 0.75 * _rttvar[k] + 0.25 * Math.Abs(_srtt[k] - x);
                    _srtt[k] = 0.875 * _srtt[k] + 0.125 * x;
                }
                _retryTokens = Math.Min(40, _retryTokens + 0.2);
                _rpcs.Remove(observation.RequestId);
                return RetryDecision.NoRetry();
            }

            // Повтор имеет смысл, только если клиент ещё может дождаться ответа.
            double remaining = PatienceMax(req.Kind) - (now - req.CreatedAt).TotalSeconds;
            bool cheapRetries = req.Kind == RequestKind.Generate; // регистрация идемпотентна и дешёвая
            int maxAttempts = cheapRetries ? 30 : 5;
            if (observation.Outcome == RequestOutcome.PolicyRejected || remaining < _srtt[k] + 0.01 ||
                observation.Attempt >= maxAttempts || (!cheapRetries && _retryTokens < 1))
            {
                _rpcs.Remove(observation.RequestId);
                return RetryDecision.NoRetry();
            }
            if (!cheapRetries) _retryTokens -= 1;

            double delay = observation.Outcome switch
            {
                RequestOutcome.Timeout => 0.0,                          // скорее всего потеря - повторяем сразу
                RequestOutcome.Rejected => cheapRetries ? 0.3 : 0.02,   // перегрузка - небольшая пауза
                _ => 0.01
            };
            delay *= 0.75 + 0.5 * random.NextDouble();
            if (cheapRetries && observation.Attempt > 3) delay = Math.Max(delay, 0.25);
            var d = TimeSpan.FromSeconds(delay);
            _rpcs[observation.RequestId] = (req, now + d);
            return RetryDecision.RetryAfter(d);
        }
    }
}
