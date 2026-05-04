using static triaxis.Data.SqlImportExport.Tests.TestHelpers;

namespace triaxis.Data.SqlImportExport.Tests;

public class BulkImportTests : SqlTestFixture
{
    [Test]
    public async Task Insert_IdentityPK_SourceSuppliesIdentity_PreservesValues()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(100,1) PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, "a"],
            [2, "b"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Is.Empty, "no synthesized identity → no inserted-id range");
        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "a"), new Row(2, "b") }));
    }

    [Test]
    public async Task Insert_IdentityPK_SourceOmitsIdentity_EmptyTable_SynthesizesFromSeed()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(100,5) PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Name"],
            ["a"],
            ["b"],
            ["c"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That(ranges[0].SourceName, Is.EqualTo("Foo"));
        Assert.That(ranges[0].First, Is.EqualTo(100));
        Assert.That(ranges[0].Last, Is.EqualTo(110));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(100, "a"), new Row(105, "b"), new Row(110, "c") }));
    }

    [Test]
    public async Task Insert_IdentityPK_SourceOmitsIdentity_PopulatedTable_ContinuesFromLastValue()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");
        await Connection.ExecAsync("INSERT Foo (Name) VALUES ('seed1'), ('seed2');");

        var src = new ListSource("Foo", ["Name"],
            ["a"],
            ["b"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That(ranges[0].First, Is.EqualTo(3));
        Assert.That(ranges[0].Last, Is.EqualTo(4));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Row(1, "seed1"),
            new Row(2, "seed2"),
            new Row(3, "a"),
            new Row(4, "b"),
        }));
    }

    [Test]
    public async Task Insert_NonIdentityPK_SourceSuppliesPK_InsertsAsIs()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Id", "Name"],
            [10, "a"],
            [20, "b"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Is.Empty);
        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(10, "a"), new Row(20, "b") }));
    }

    [Test]
    public async Task Insert_NoPK_NoIdentity_Works()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Name nvarchar(50), Value int)");

        var src = new ListSource("Foo", ["Name", "Value"],
            ["a", 1],
            ["b", 2]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var count = (await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single();
        Assert.That(count, Is.EqualTo(2));
    }

    [Test]
    public async Task Insert_KeepNullsTrue_PreservesNulls()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, DBNull.Value]);

        await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { KeepNulls = true });

        var name = (await Connection.ReadAsync<string?>("SELECT Name FROM Foo")).Single();
        Assert.That(name, Is.Null);
    }

    [Test]
    public async Task Insert_KeepNullsFalse_AppliesDefault()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, DBNull.Value]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var name = (await Connection.ReadAsync<string?>("SELECT Name FROM Foo")).Single();
        Assert.That(name, Is.EqualTo("def"));
    }

    [Test]
    public async Task Insert_DryRun_RollsBack()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Id", "Name"], [1, "a"]);

        await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { DryRun = true });

        var count = (await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single();
        Assert.That(count, Is.Zero);
    }

    [Test]
    public async Task Insert_MultiBatch_AllRowsArrive()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, N int)");

        var rows = Enumerable.Range(0, 2500).Select(i => new object[] { i }).ToArray();
        var src = new ListSource("Foo", ["N"], rows);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { BatchSize = 500 });

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That(ranges[0].First, Is.EqualTo(1));
        Assert.That(ranges[0].Last, Is.EqualTo(2500));

        var count = (await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single();
        Assert.That(count, Is.EqualTo(2500));
    }

    [Test]
    public async Task Truncate_ReplacesExistingRows()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");
        await Connection.ExecAsync("INSERT Foo VALUES (1, 'old'), (2, 'old');");

        var src = new ListSource("Foo", ["Id", "Name"], [10, "new"]) { Strategy = BulkImportStrategy.Truncate };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(10, "new") }));
    }

    [Test]
    public async Task Upsert_InsertsNewAndUpdatesExisting()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");
        await Connection.ExecAsync("INSERT Foo VALUES (1, 'old'), (2, 'old');");

        var src = new ListSource("Foo", ["Id", "Name"],
            [2, "updated"],
            [3, "new"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Row(1, "old"),
            new Row(2, "updated"),
            new Row(3, "new"),
        }));
    }

    [Test]
    public async Task InsertIgnore_InsertsNewSkipsExisting()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");
        await Connection.ExecAsync("INSERT Foo VALUES (1, 'old'), (2, 'old');");

        var src = new ListSource("Foo", ["Id", "Name"],
            [2, "should-be-ignored"],
            [3, "new"]) { Strategy = BulkImportStrategy.InsertIgnore };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Row(1, "old"),
            new Row(2, "old"),
            new Row(3, "new"),
        }));
    }

    [Test]
    public async Task PerSourceStrategy_OverridesGlobal()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");
        await Connection.ExecAsync("INSERT Foo VALUES (1, 'old');");

        // global = Upsert; per-source override = InsertIgnore (existing row stays as 'old')
        var src = new ListSource("Foo", ["Id", "Name"], [1, "ignored-by-source-override"])
        {
            Strategy = BulkImportStrategy.InsertIgnore,
        };

        await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { Strategy = BulkImportStrategy.Upsert });

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "old") }));
    }

    [Test]
    public async Task Reference_IdentityPK_SourceSuppliesIdentity_ResolvesToSuppliedValues()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE Child  (Id int IDENTITY(1,1) PRIMARY KEY, ParentId int NOT NULL FOREIGN KEY REFERENCES Parent(Id), Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Id", "Name"],
            [10, "p0"],
            [20, "p1"]);

        var child = new ListSource("Child", ["ParentId", "Tag"],
            [new BulkImportSourceReference(parent, 0), "c-of-p0"],
            [new BulkImportSourceReference(parent, 1), "c-of-p1"],
            [new BulkImportSourceReference(parent, 0), "c2-of-p0"]);

        await Service.BulkImportAsync(Connection, AsAsync(parent, child));

        var pairs = await Connection.ReadAsync<Pair>(
            "SELECT c.Tag, p.Name FROM Child c JOIN Parent p ON p.Id = c.ParentId ORDER BY c.Id");
        Assert.That(pairs, Is.EqualTo(new[]
        {
            new Pair("c-of-p0", "p0"),
            new Pair("c-of-p1", "p1"),
            new Pair("c2-of-p0", "p0"),
        }));
    }

    [Test]
    public async Task Reference_IdentityPK_SourceOmitsIdentity_ResolvesToSynthesizedValues()
    {
        // the original bug repro: default options (no SkipIdentity), multi-row source A,
        // source B references A by row index — previously threw.
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE Child  (Id int IDENTITY(1,1) PRIMARY KEY, ParentId int NOT NULL FOREIGN KEY REFERENCES Parent(Id), Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Name"], ["p0"], ["p1"]);
        var child = new ListSource("Child", ["ParentId", "Tag"],
            [new BulkImportSourceReference(parent, 0), "c-of-p0"],
            [new BulkImportSourceReference(parent, 1), "c-of-p1"]);

        await Service.BulkImportAsync(Connection, AsAsync(parent, child));

        var pairs = await Connection.ReadAsync<Pair>(
            "SELECT c.Tag, p.Name FROM Child c JOIN Parent p ON p.Id = c.ParentId ORDER BY c.Id");
        Assert.That(pairs, Is.EqualTo(new[]
        {
            new Pair("c-of-p0", "p0"),
            new Pair("c-of-p1", "p1"),
        }));
    }

    [Test]
    public async Task Reference_NonIdentityPK_ResolvesFromCapturedRows()
    {
        // the second bug repro: target has no IDENTITY column so the old IDENT_CURRENT path
        // returned nothing, no map entry was added, and references threw.
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Code int PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE Child  (Id int IDENTITY(1,1) PRIMARY KEY, ParentCode int NOT NULL FOREIGN KEY REFERENCES Parent(Code), Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Code", "Name"],
            [42, "p0"],
            [99, "p1"]);

        var child = new ListSource("Child", ["ParentCode", "Tag"],
            [new BulkImportSourceReference(parent, 0), "c-of-p0"],
            [new BulkImportSourceReference(parent, 1), "c-of-p1"]);

        await Service.BulkImportAsync(Connection, AsAsync(parent, child));

        var pairs = await Connection.ReadAsync<Pair>(
            "SELECT c.Tag, p.Name FROM Child c JOIN Parent p ON p.Code = c.ParentCode ORDER BY c.Id");
        Assert.That(pairs, Is.EqualTo(new[]
        {
            new Pair("c-of-p0", "p0"),
            new Pair("c-of-p1", "p1"),
        }));
    }

    [Test]
    public async Task Reference_ToUnprocessedSource_Throws()
    {
        await Connection.ExecAsync("CREATE TABLE Bar (Id int PRIMARY KEY)");

        var unprocessed = new ListSource("Other", ["Id"]);
        var src = new ListSource("Bar", ["Id"],
            [new BulkImportSourceReference(unprocessed, 0)]);

        Assert.That(
            async () => await Service.BulkImportAsync(Connection, AsAsync(src)),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task Reference_CompositePK_WithIdentity_SourceOmitsIdentity_ResolvesViaFormula()
    {
        // composite PK so capture can't fire (more than one PK column);
        // identity column omitted by source so synthesis kicks in and the formula becomes the resolver.
        await Connection.ExecAsync("""
            CREATE TABLE Parent (
                Id int IDENTITY(1,1),
                Region nvarchar(10) NOT NULL,
                Name nvarchar(50),
                CONSTRAINT PK_Parent PRIMARY KEY (Region, Id));
            CREATE TABLE Child (Id int IDENTITY(1,1) PRIMARY KEY, ParentId int, Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Region", "Name"],
            ["us", "p0"],
            ["us", "p1"]);

        var child = new ListSource("Child", ["ParentId", "Tag"],
            [new BulkImportSourceReference(parent, 0), "c-of-p0"],
            [new BulkImportSourceReference(parent, 1), "c-of-p1"]);

        await Service.BulkImportAsync(Connection, AsAsync(parent, child));

        var rows = await Connection.ReadAsync<Row>("SELECT ParentId, Tag FROM Child ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "c-of-p0"), new Row(2, "c-of-p1") }));
    }

    [Test]
    public async Task Reference_CompositePK_SourceSuppliesIdentity_NoResolverRegistered_Throws()
    {
        // composite PK + source supplies identity column → no synthesis, no single-PK capture, no resolver.
        await Connection.ExecAsync("""
            CREATE TABLE Parent (
                Id int IDENTITY(1,1),
                Region nvarchar(10) NOT NULL,
                CONSTRAINT PK_Parent PRIMARY KEY (Region, Id));
            CREATE TABLE Bar (Value int);
            """);

        var parent = new ListSource("Parent", ["Id", "Region"], [10, "us"]);
        var bar = new ListSource("Bar", ["Value"],
            [new BulkImportSourceReference(parent, 0)]);

        Assert.That(
            async () => await Service.BulkImportAsync(Connection, AsAsync(parent, bar)),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task Reference_NoPK_NoIdentity_Throws()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Plain (Name nvarchar(50));
            CREATE TABLE Bar (Value nvarchar(50));
            """);

        var plain = new ListSource("Plain", ["Name"], ["x"]);
        var bar = new ListSource("Bar", ["Value"],
            [new BulkImportSourceReference(plain, 0)]);

        Assert.That(
            async () => await Service.BulkImportAsync(Connection, AsAsync(plain, bar)),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task Reference_IdentityNotPK_CapturesPKAndSynthesizesIdentity()
    {
        // single PK is a non-identity column (Code); a separate identity column (AutoId) exists.
        // source supplies Code but omits AutoId — synthesis fires for AutoId, capture fires for Code.
        await Connection.ExecAsync("""
            CREATE TABLE Parent (
                Code int PRIMARY KEY,
                AutoId int IDENTITY(1,1) NOT NULL UNIQUE,
                Name nvarchar(50));
            CREATE TABLE Child (
                Id int IDENTITY(1,1) PRIMARY KEY,
                ParentCode int NOT NULL FOREIGN KEY REFERENCES Parent(Code),
                Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Code", "Name"],
            [42, "p0"],
            [99, "p1"]);

        var child = new ListSource("Child", ["ParentCode", "Tag"],
            [new BulkImportSourceReference(parent, 0), "c-of-p0"],
            [new BulkImportSourceReference(parent, 1), "c-of-p1"]);

        await Service.BulkImportAsync(Connection, AsAsync(parent, child));

        var pairs = await Connection.ReadAsync<Pair>(
            "SELECT c.Tag, p.Name FROM Child c JOIN Parent p ON p.Code = c.ParentCode ORDER BY c.Id");
        Assert.That(pairs, Is.EqualTo(new[] { new Pair("c-of-p0", "p0"), new Pair("c-of-p1", "p1") }));

        // AutoId was synthesized starting at 1 (seed) since the table was empty.
        var autoIds = await Connection.ReadAsync<int>("SELECT AutoId FROM Parent ORDER BY Code");
        Assert.That(autoIds, Is.EqualTo(new[] { 1, 2 }));
    }

    [Test]
    public async Task Insert_EmptySource_NoRangeNoRowsNoMapEntry()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");

        var empty = new ListSource("Foo", ["Name"]);
        var ranges = await Service.BulkImportAsync(Connection, AsAsync(empty));

        Assert.That(ranges, Is.Empty);
        Assert.That(
            (await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(),
            Is.Zero);
    }

    [Test]
    public async Task Reference_ToEmptySource_Throws()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE Bar (Value int);
            """);

        var emptyParent = new ListSource("Parent", ["Name"]);
        var bar = new ListSource("Bar", ["Value"],
            [new BulkImportSourceReference(emptyParent, 0)]);

        Assert.That(
            async () => await Service.BulkImportAsync(Connection, AsAsync(emptyParent, bar)),
            Throws.InvalidOperationException);
    }

    [Test]
    public async Task MixedStrategies_InOneCall()
    {
        await Connection.ExecAsync("""
            CREATE TABLE A (Id int PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE B (Id int PRIMARY KEY, Name nvarchar(50));
            INSERT A VALUES (1, 'old');
            INSERT B VALUES (1, 'old'), (2, 'old');
            """);

        var truncSrc = new ListSource("A", ["Id", "Name"], [9, "new"]) { Strategy = BulkImportStrategy.Truncate };
        var upsertSrc = new ListSource("B", ["Id", "Name"], [2, "updated"], [3, "added"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(truncSrc, upsertSrc));

        Assert.That(await Connection.ReadAsync<Row>("SELECT Id, Name FROM A ORDER BY Id"),
            Is.EqualTo(new[] { new Row(9, "new") }));
        Assert.That(await Connection.ReadAsync<Row>("SELECT Id, Name FROM B ORDER BY Id"),
            Is.EqualTo(new[] { new Row(1, "old"), new Row(2, "updated"), new Row(3, "added") }));
    }

    [Test]
    public async Task AutoOpenConnection_OpensAndClosesAroundCall()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY)");

        await using var closed = new SqlConnection(TestConnectionString);
        Assert.That(closed.State, Is.EqualTo(System.Data.ConnectionState.Closed));

        var src = new ListSource("Foo", ["Id"], [1]);
        await Service.BulkImportAsync(closed, AsAsync(src));

        Assert.That(closed.State, Is.EqualTo(System.Data.ConnectionState.Closed));
        Assert.That(
            (await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(),
            Is.EqualTo(1));
    }

    [Test]
    public async Task Truncate_ResetsIdentitySeed_SynthesisStartsFromSeed()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(50,1) PRIMARY KEY, Name nvarchar(50));
            INSERT Foo (Name) VALUES ('x'), ('y'), ('z');  -- seed advances to 52
            """);

        var src = new ListSource("Foo", ["Name"], ["a"], ["b"]) { Strategy = BulkImportStrategy.Truncate };
        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That(ranges[0].First, Is.EqualTo(50), "TRUNCATE resets identity → synthesis starts at seed");
        Assert.That(ranges[0].Last, Is.EqualTo(51));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(50, "a"), new Row(51, "b") }));
    }

    [Test]
    public async Task Upsert_IdentityTable_PreservesSuppliedIdentities()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50));
            INSERT Foo (Name) VALUES ('first');  -- id = 1
            """);

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, "updated"],
            [5, "new"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "updated"), new Row(5, "new") }));
    }

    [Test]
    public async Task Merge_KeepNullsFalse_AppliesDefaultsViaTempTable()
    {
        // exercises the merge-prep default-constraint replication: temp table needs the default
        // copied across so that bulk copy substitutes NULLs from the source with the default.
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50), Status nvarchar(20) DEFAULT 'active');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Status"],
            [1, "a", DBNull.Value]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var status = (await Connection.ReadAsync<string?>("SELECT Status FROM Foo")).Single();
        Assert.That(status, Is.EqualTo("active"));
    }

    [Test]
    public async Task Insert_BigIntIdentity_SynthesizesCorrectly()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id bigint IDENTITY(1000000000000,1) PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Name"], ["a"], ["b"]);
        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That(ranges[0].First, Is.EqualTo(1000000000000L));
        Assert.That(ranges[0].Last, Is.EqualTo(1000000000001L));

        var rows = await Connection.ReadAsync<BigIntRow>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new BigIntRow(1000000000000L, "a"),
            new BigIntRow(1000000000001L, "b"),
        }));
    }

    [Test]
    public async Task Insert_SynthesisAdvancesSeed_RegularInsertAndSecondImportContinue()
    {
        // verify our manual identity injection (SET IDENTITY_INSERT path) leaves SQL Server's
        // identity counter consistent: a regular INSERT and a subsequent synthesis-only import
        // should both pick up where the previous step left off.
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");

        var first = new ListSource("Foo", ["Name"], ["a"], ["b"]);
        var firstRanges = await Service.BulkImportAsync(Connection, AsAsync(first));
        Assert.That(firstRanges[0].First, Is.EqualTo(1));
        Assert.That(firstRanges[0].Last, Is.EqualTo(2));

        await Connection.ExecAsync("INSERT Foo (Name) VALUES ('manual');");

        var second = new ListSource("Foo", ["Name"], ["c"], ["d"]);
        var secondRanges = await Service.BulkImportAsync(Connection, AsAsync(second));
        Assert.That(secondRanges[0].First, Is.EqualTo(4));
        Assert.That(secondRanges[0].Last, Is.EqualTo(5));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Row(1, "a"), new Row(2, "b"),
            new Row(3, "manual"),
            new Row(4, "c"), new Row(5, "d"),
        }));
    }

    [Test]
    public async Task SkipConstraints_False_RechecksAndTrustsForeignKeys()
    {
        // create an FK that's marked NOT TRUSTED (e.g. via WITH NOCHECK), then verify the import re-trusts it.
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int PRIMARY KEY);
            CREATE TABLE Child  (Id int PRIMARY KEY, ParentId int);
            ALTER TABLE Child WITH NOCHECK ADD CONSTRAINT FK_Child_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id);
            INSERT Parent VALUES (1);
            """);

        Assert.That(
            (await Connection.ReadAsync<int>("SELECT CONVERT(int, is_not_trusted) FROM sys.foreign_keys WHERE name = 'FK_Child_Parent'")).Single(),
            Is.EqualTo(1), "precondition: FK is not trusted");

        var src = new ListSource("Child", ["Id", "ParentId"], [1, 1]);
        await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(
            (await Connection.ReadAsync<int>("SELECT CONVERT(int, is_not_trusted) FROM sys.foreign_keys WHERE name = 'FK_Child_Parent'")).Single(),
            Is.Zero, "post: FK should be re-trusted");
    }

    [Test]
    public async Task SkipConstraints_True_LeavesUntrustedForeignKeysAlone()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int PRIMARY KEY);
            CREATE TABLE Child  (Id int PRIMARY KEY, ParentId int);
            ALTER TABLE Child WITH NOCHECK ADD CONSTRAINT FK_Child_Parent FOREIGN KEY (ParentId) REFERENCES Parent(Id);
            INSERT Parent VALUES (1);
            """);

        var src = new ListSource("Child", ["Id", "ParentId"], [1, 1]);
        await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { SkipConstraints = true });

        Assert.That(
            (await Connection.ReadAsync<int>("SELECT CONVERT(int, is_not_trusted) FROM sys.foreign_keys WHERE name = 'FK_Child_Parent'")).Single(),
            Is.EqualTo(1), "FK should still be untrusted");
    }

    private record Row(int Id, string Name);
    private record Pair(string Left, string Right);
    private record BigIntRow(long Id, string Name);
}
