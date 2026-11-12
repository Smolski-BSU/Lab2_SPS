// Стенд для проверки стратегии: открытый сценарий + несколько "продвинутых" сценариев с отказами.
// Запуск: dotnet run -c Release --project SPS.Bench -- [mine|sample] [all|префикс_сценария] [число_seed] [v]

using System.Globalization;
using SPS.Abstractions;
using SPS.Simulation;
using SPS.Simulation.Public;
using SPS.Student;

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
string policyName = args.Length > 0 ? args[0] : "mine";
string scen = args.Length > 1 ? args[1] : "all";
int seeds = args.Length > 2 ? int.Parse(args[2]) : 20;
bool verbose = args.Length > 3 && args[3] == "v";

ISystemPolicy Make() => policyName switch
{
    "sample" => new SampleSystemPolicy(),
    _ => new ProfitAwareSystemPolicy(),
};

var phasesPub = PublicScenario.Create().Phases;
SimulationScenario Adv(string name, IReadOnlyList<WorkloadPhase> phases, FaultInterval[] faults, bool variants, double loss = .005)
{
    var profiles = PublicScenario.CreateProfiles(true);
    if (variants)
    {
        profiles = Array.AsReadOnly(new[]
        {
            new RequestProfile(RequestKind.Metadata, .05m, .5, 1.5, 0, .015, 1, 1, 2, 1024)
                { Variants = new[] { new RequestVariant(.04m, 3), new RequestVariant(.08m, 1), new RequestVariant(.05m, 2) } },
            new RequestProfile(RequestKind.Preview, .20m, 1, 3, .015, .060, 1, 1, 5, 16384)
                { Variants = new[] { new RequestVariant(.15m, 8), new RequestVariant(.35m, 2), new RequestVariant(.2m, 5) } },
            new RequestProfile(RequestKind.Generate, 5m, 15, 60, .010, .120, 2, 12, 1, 1024)
                { Variants = new[] { new RequestVariant(2m, 1), new RequestVariant(5m, 1), new RequestVariant(9m, 1, .5) } },
        });
    }
    return new SimulationScenario(name, TimeSpan.FromSeconds(180), PublicScenario.CreateSettings(true), profiles, phases, faults)
        { ResourceKeyCount = 80, ResponseLossProbability = loss };
}
SimulationScenario AdvS(string name, IReadOnlyList<WorkloadPhase> phases, FaultInterval[] faults)
{
    var b = PublicScenario.CreateSettings(true);
    var settings = new SystemSettings(new(4, 32, 1, .2m, 1, TimeSpan.FromSeconds(2.5), TimeSpan.FromSeconds(2)),
        new(1, 0, 1, .6m, 2, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10)), 8, 8, 20, 120_000, 500, 2, 64,
        TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(500), TimeSpan.FromMilliseconds(250));
    var profiles = Array.AsReadOnly(new[]
    {
        new RequestProfile(RequestKind.Metadata, .05m, .5, 1.5, 0, .015, 1, 1, 2, 1024) { Variants = new[] { new RequestVariant(.05m, 1), new RequestVariant(.1m, .5) } },
        new RequestProfile(RequestKind.Preview, .20m, 1, 3, .015, .060, 1, 1, 5, 16384) { Variants = new[] { new RequestVariant(.2m, 3), new RequestVariant(.4m, 1.5) } },
        new RequestProfile(RequestKind.Generate, 4m, 15, 60, .010, .120, 2, 12, 1, 1024) { Variants = new[] { new RequestVariant(1.5m, 1), new RequestVariant(4m, 1), new RequestVariant(8m, 1) } },
    });
    return new SimulationScenario(name, TimeSpan.FromSeconds(180), settings, profiles, phases, faults) { ResourceKeyCount = 120, ResponseLossProbability = .01 };
}
var faultsA = new[]
{
    new FaultInterval(30, 36, FaultKind.Slowdown, InstanceRole.Api, 0, 4),
    new FaultInterval(45, 52, FaultKind.Crash, InstanceRole.Api, 1),
    new FaultInterval(60, 70, FaultKind.HeartbeatIsolation, InstanceRole.Worker, 0),
    new FaultInterval(80, 90, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 3),
    new FaultInterval(98, 110, FaultKind.Crash, InstanceRole.Worker, 1),
    new FaultInterval(120, 130, FaultKind.NetworkLoss, InstanceRole.Api, 0),
    new FaultInterval(140, Environment.GetEnvironmentVariable("LONGSLOW") != null ? 175 : 150, FaultKind.Slowdown, InstanceRole.Worker, 2, 4), new FaultInterval(Environment.GetEnvironmentVariable("LONGSLOW") != null ? 20 : 300, Environment.GetEnvironmentVariable("LONGSLOW") != null ? 80 : 301, FaultKind.Slowdown, InstanceRole.Worker, 0, 3),
    new FaultInterval(155, 160, FaultKind.NetworkIsolation, InstanceRole.Worker, 0),
};
var phasesB = new[]
{
    new WorkloadPhase(0, 20, .06, .30, .8),
    new WorkloadPhase(30, 45, .10, .40, .9, 10, 6),
    new WorkloadPhase(80, 25, .05, .25, .6, 30, 8),
    new WorkloadPhase(120, 35, .12, .30, .85, 55, 4),
    new WorkloadPhase(160, 10, .04, .30, .7, 5),
};
var fi = Environment.GetEnvironmentVariable("FAULT");
if (fi != null) faultsA = new[] { faultsA[int.Parse(fi)] };
var scenarios = new List<SimulationScenario>
{
    PublicScenario.Create(),
    Adv("adv-pubphases", phasesPub, faultsA, false),
    Adv("adv-variants", phasesB, faultsA, true, .01),
    Adv("adv-apicrash", phasesB, new[] { new FaultInterval(50, 62, FaultKind.Crash, InstanceRole.Api, 0), new FaultInterval(100, 104, FaultKind.NetworkIsolation, InstanceRole.Api, 0), new FaultInterval(130, 140, FaultKind.Crash, InstanceRole.Worker, 0), new FaultInterval(140, 150, FaultKind.Crash, InstanceRole.Worker, 1) }, true),
    Adv("adv-light", new[] { new WorkloadPhase(0, 15, .03, .3, .8), new WorkloadPhase(40, 30, .05, .35, .9, 20), new WorkloadPhase(90, 10, .08, .3, .7, 40), new WorkloadPhase(130, 25, .02, .3, .85, 60) }, faultsA, true),
    Adv("adv-heavy", new[] { new WorkloadPhase(0, 80, .04, .6, .5, 0, 10), new WorkloadPhase(60, 150, .03, .7, .3, 20, 20), new WorkloadPhase(120, 60, .06, .5, .9, 40, 3) }, faultsA, true),
    AdvS("adv-pricey", phasesB, faultsA),
    Adv("adv-storage", phasesPub, new[] { new FaultInterval(20, 80, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 3), new FaultInterval(100, 130, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 6), new FaultInterval(98, 102, FaultKind.Crash, InstanceRole.Worker, 1) }, true),
    Adv("edge-nogen", new[] { new WorkloadPhase(0, 30, 0, .5, .5) }, Array.Empty<FaultInterval>(), true),
    Adv("edge-allgen", new[] { new WorkloadPhase(0, 3, 1, 0, .5), new WorkloadPhase(90, 0.2, 1, 0, .5) }, faultsA, true),
    Adv("adv-calm", phasesB, Array.Empty<FaultInterval>(), true),
};
// Стресс-сценарии (запуск: ... mine stress 20): другие лимиты и настройки, длинные и множественные отказы.
SystemSettings Set(int maxApis = 8, int maxWorkers = 8, int cu = 20, long cache = 196_608, int storageQueue = 64,
    double hb = .5, double review = .25, double capReview = 1, double probe = .25)
{
    var b = PublicScenario.CreateSettings(true);
    return new SystemSettings(b.Api, b.Worker, maxApis, maxWorkers, cu, cache, 500, 2, storageQueue,
        TimeSpan.FromSeconds(review), TimeSpan.FromSeconds(capReview), TimeSpan.FromSeconds(hb), TimeSpan.FromSeconds(probe));
}
SimulationScenario St(string name, SystemSettings settings, IReadOnlyList<WorkloadPhase> phases, FaultInterval[] faults, double loss = .005) =>
    new(name, TimeSpan.FromSeconds(180), settings, PublicScenario.CreateProfiles(true), phases, faults) { ResourceKeyCount = 80, ResponseLossProbability = loss };
var stress = new List<SimulationScenario>
{
    St("st-storagefull", Set(), new[] { new WorkloadPhase(0, 60, .08, .6, .5, 0, 10), new WorkloadPhase(60, 120, .06, .7, .3, 20, 20), new WorkloadPhase(120, 40, .1, .5, .9, 40, 3) },
        new[] { new FaultInterval(40, 90, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 8), new FaultInterval(120, 150, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 5) }),
    St("st-apiiso", Set(), phasesPub, new[] { new FaultInterval(40, 60, FaultKind.NetworkIsolation, InstanceRole.Api, 0), new FaultInterval(100, 130, FaultKind.NetworkLoss, InstanceRole.Api, 0), new FaultInterval(140, 150, FaultKind.Slowdown, InstanceRole.Api, 0, 10) }),
    St("st-hblong", Set(), phasesPub, new[] { new FaultInterval(30, 70, FaultKind.HeartbeatIsolation, InstanceRole.Worker, 0), new FaultInterval(50, 90, FaultKind.HeartbeatIsolation, InstanceRole.Worker, 1), new FaultInterval(100, 140, FaultKind.HeartbeatIsolation, InstanceRole.Worker, 2) }),
    St("st-multicrash", Set(), phasesPub, new[] { new FaultInterval(60, 80, FaultKind.Crash, InstanceRole.Worker, 0), new FaultInterval(60, 80, FaultKind.Crash, InstanceRole.Worker, 1), new FaultInterval(62, 85, FaultKind.Crash, InstanceRole.Worker, 2), new FaultInterval(65, 75, FaultKind.Crash, InstanceRole.Worker, 3), new FaultInterval(90, 100, FaultKind.Crash, InstanceRole.Api, 0), new FaultInterval(92, 98, FaultKind.Crash, InstanceRole.Api, 1) }),
    St("st-tiny", Set(maxApis: 2, maxWorkers: 2, cu: 6), phasesPub, new[] { new FaultInterval(50, 55, FaultKind.Crash, InstanceRole.Api, 0), new FaultInterval(100, 110, FaultKind.Crash, InstanceRole.Worker, 0) }),
    St("st-nocache", Set(cache: 4096), phasesPub, Array.Empty<FaultInterval>()),
    St("st-hb2s", Set(hb: 2, review: .5, capReview: 2, probe: .5), phasesPub, new[] { new FaultInterval(45, 50, FaultKind.Crash, InstanceRole.Api, 0), new FaultInterval(70, 90, FaultKind.HeartbeatIsolation, InstanceRole.Worker, 0), new FaultInterval(100, 115, FaultKind.Crash, InstanceRole.Worker, 1) }),
    St("st-slowworkers", Set(), phasesPub, new[] { new FaultInterval(0, 180, FaultKind.Slowdown, InstanceRole.Worker, 0, 4), new FaultInterval(0, 180, FaultKind.Slowdown, InstanceRole.Worker, 1, 3) }),
    St("st-surge", Set(), new[] { new WorkloadPhase(0, 10, .1, .3, .8), new WorkloadPhase(40, 12, .5, .3, .8), new WorkloadPhase(80, 6, .05, .3, .8), new WorkloadPhase(120, 8, .4, .3, .8) }, Array.Empty<FaultInterval>()),
    St("st-loss5", Set(), phasesPub, new[] { new FaultInterval(45, 49, FaultKind.Crash, InstanceRole.Api, 1), new FaultInterval(98, 102, FaultKind.Crash, InstanceRole.Worker, 1) }, .05),
    St("st-smallq", Set(storageQueue: 8), new[] { new WorkloadPhase(0, 40, .08, .5, .6, 0, 10), new WorkloadPhase(90, 80, .06, .6, .4, 20, 20) }, new[] { new FaultInterval(30, 60, FaultKind.StorageSlowdown, InstanceRole.Api, 0, 4) }),
};
if (scen == "stress") { scenarios = stress; scen = "all"; }
else if (scen.StartsWith("st-")) scenarios = stress;

var means = new List<string>(); decimal tot = 0;
foreach (var s in scenarios)
{
    if (scen != "all" && !s.Name.StartsWith(scen)) continue;
    decimal sum = 0; var profits = new List<decimal>();
    decimal rev = 0, api = 0, wrk = 0; var byKind = new decimal[3]; var pot = new decimal[3];
    for (int seed = 1; seed <= seeds; seed++)
    {
        var r = new SimulationRunner().Run(s, Make(), new SimulationOptions(seed));
        var p = r.Economy.Revenue - r.Economy.ApiCost - r.Economy.WorkerCost;
        profits.Add(p); sum += p; rev += r.Economy.Revenue; api += r.Economy.ApiCost; wrk += r.Economy.WorkerCost;
        foreach (var k in r.RequestTypes) { byKind[(int)k.Kind] += k.EarnedRevenue; pot[(int)k.Kind] += k.PotentialRevenue; }
        if (verbose)
        {
            var d = r.Diagnostics;
            Console.WriteLine($"  seed {seed}: profit {p:F1} rev {r.Economy.Revenue:F1} api {r.Economy.ApiCost:F1} wrk {r.Economy.WorkerCost:F1} | " +
                string.Join(" ", r.RequestTypes.Select(k => $"{k.Kind.ToString()[0]}:{k.OnTimeRate:P0}")) +
                $" | att {d.Attempts} ret {d.Retries} to {d.Timeouts} rej {d.Rejections} prej {d.PolicyRejections} hit {d.CacheHits} coal {d.CoalescedCustomers} stale {d.StaleResponses} reas {d.JobReassignments} dup {d.DuplicateJobExecutions} waste {d.WastedInstanceTime.TotalSeconds:F0}s");
        }
    }
    int n = profits.Count; means.Add($"{sum / n:F1}"); tot += sum / n;
    if (Environment.GetEnvironmentVariable("BRIEF") == null) Console.WriteLine($"{s.Name,-16} {policyName}: mean profit {sum / n,8:F2}  min {profits.Min(),8:F2}  rev {rev / n:F1} api {api / n:F1} wrk {wrk / n:F1} | M {byKind[0] / n:F1}/{pot[0] / n:F1} P {byKind[1] / n:F1}/{pot[1] / n:F1} G {byKind[2] / n:F1}/{pot[2] / n:F1}");
}

if (Environment.GetEnvironmentVariable("BRIEF") != null) Console.WriteLine("BRIEF " + string.Join(" ", means) + $" | total {tot:F1}");

