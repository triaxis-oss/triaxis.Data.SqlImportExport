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
    public async Task Insert_DBNullCell_ColumnWithDefault_InsertsNull()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, DBNull.Value]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var name = (await Connection.ReadAsync<string?>("SELECT Name FROM Foo")).Single();
        Assert.That(name, Is.Null, "DBNull means a literal NULL, default or not");
    }

    [Test]
    public async Task Insert_NullCell_ColumnWithDefault_AppliesDefault()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, null]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var name = (await Connection.ReadAsync<string?>("SELECT Name FROM Foo")).Single();
        Assert.That(name, Is.EqualTo("def"), "a null cell behaves like an INSERT omitting the column");
    }

    [Test]
    public async Task Insert_NullCell_ColumnWithoutDefault_InsertsNull()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, null]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var name = (await Connection.ReadAsync<string?>("SELECT Name FROM Foo")).Single();
        Assert.That(name, Is.Null);
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
    public async Task Merge_NullCell_InsertAppliesDefault()
    {
        // NOT NULL matters: the staging buffer has to hold the NULL standing in for "omitted"
        // even where the real column would reject it
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50), Status nvarchar(20) NOT NULL DEFAULT 'active');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Status"],
            [1, "a", null]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var status = (await Connection.ReadAsync<string?>("SELECT Status FROM Foo")).Single();
        Assert.That(status, Is.EqualTo("active"));
    }

    [Test]
    public async Task Upsert_NullCell_MatchedRow_LeavesColumnUntouched()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50), Status nvarchar(20) DEFAULT 'active');
            INSERT Foo VALUES (1, 'old', 'kept');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Status"],
            [1, "updated", null]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, Status FROM Foo");
        Assert.That(rows, Is.EqualTo(new[] { new Triple(1, "updated", "kept") }),
            "a null cell means 'value not present' - the update must not touch the column");
    }

    [Test]
    public async Task Upsert_DBNullCell_MatchedRow_OverwritesWithNull()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50), Status nvarchar(20) DEFAULT 'active');
            INSERT Foo VALUES (1, 'old', 'kept');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Status"],
            [1, "updated", DBNull.Value]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, Status FROM Foo");
        Assert.That(rows, Is.EqualTo(new[] { new Triple(1, "updated", null) }));
    }

    [Test]
    public async Task Merge_DBNullCell_ColumnWithDefault_InsertsNull()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50), Status nvarchar(20) DEFAULT 'active');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Status"],
            [1, "a", DBNull.Value]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var status = (await Connection.ReadAsync<string?>("SELECT Status FROM Foo")).Single();
        Assert.That(status, Is.Null, "DBNull survives the staging table despite the default");
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

    [Test]
    public async Task Insert_TwoSourcesSameTable_SynthesisContinuesAcrossSources()
    {
        // the seed is tracked in-process across sources instead of re-read per source,
        // so the second source has to pick up where the first one stopped
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");

        var first = new ListSource("Foo", ["Name"], ["a"], ["b"]);
        var second = new ListSource("Foo", ["Name"], ["c"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(first, second));

        Assert.That(ranges.Select(r => (r.First, r.Last)), Is.EqualTo(new[] { (1L, 2L), (3L, 3L) }));
        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "a"), new Row(2, "b"), new Row(3, "c") }));
    }

    [Test]
    public async Task Insert_SuppliedIdentityThenSynthesized_RereadsSeed()
    {
        // the first source moves the counter somewhere we didn't compute, so the tracked value
        // has to be dropped and read back rather than assumed
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");

        var supplied = new ListSource("Foo", ["Id", "Name"], [100, "a"]);
        var synthesized = new ListSource("Foo", ["Name"], ["b"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(supplied, synthesized));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That(ranges[0].First, Is.EqualTo(101));
        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(100, "a"), new Row(101, "b") }));
    }

    [Test]
    public async Task Insert_MixedRowShapes_NullCellsTakeDefaults_DBNullStaysNull()
    {
        // one source, rows carrying different subsets of the declared column union - each
        // distinct shape goes to the server as its own bulk copy with the omitted columns
        // unmapped, while DBNull stays a literal NULL within whatever shape it rides in
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50) DEFAULT 'def', Tag nvarchar(20) DEFAULT 'tag');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Tag"],
            [1, "supplied", "supplied-tag"],
            [2, "supplied", null],
            [3, null, null],
            [4, DBNull.Value, null]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, Tag FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Triple(1, "supplied", "supplied-tag"),
            new Triple(2, "supplied", "tag"),
            new Triple(3, "def", "tag"),
            new Triple(4, null, "tag"),
        }));
    }

    [Test]
    public async Task Insert_MixedRowShapes_IdentitySynthesisSpansRuns()
    {
        // the synthesized identity values and the returned range have to run through all the
        // per-shape bulk copies as if they were one
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) DEFAULT 'def', Value int);
            """);

        var src = new ListSource("Foo", ["Name", "Value"],
            ["a", 1],
            ["b", null],
            [null, 3],
            ["d", 4]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That((ranges[0].First, ranges[0].Last), Is.EqualTo((1L, 4L)));

        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, CONVERT(nvarchar(20), Value) FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Triple(1, "a", "1"),
            new Triple(2, "b", null),
            new Triple(3, "def", "3"),
            new Triple(4, "d", "4"),
        }));
    }

    [Test]
    public async Task Insert_InterleavedShapes_BuffersAndRegroups_KeepsValuesAndIdentity()
    {
        // a source fitting the buffer is regrouped into one bulk copy per distinct shape; the
        // synthesized identity values and the returned range have to come out as if the rows
        // had streamed through in order
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) NOT NULL DEFAULT 'def', Value int);
            """);

        var src = new ListSource("Foo", ["Name", "Value"],
            ["a", 1],
            [null, 2],
            ["c", null],
            ["d", 4],
            [null, 5],
            [null, null]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges, Has.Count.EqualTo(1));
        Assert.That((ranges[0].First, ranges[0].Last), Is.EqualTo((1L, 6L)));

        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, CONVERT(nvarchar(20), Value) FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Triple(1, "a", "1"),
            new Triple(2, "def", "2"),
            new Triple(3, "c", null),
            new Triple(4, "d", "4"),
            new Triple(5, "def", "5"),
            new Triple(6, "def", null),
        }));
    }

    [Test]
    public async Task Insert_InterleavedShapes_NoBuffering_StreamsEveryRun()
    {
        // MaxBufferedRows = 0 is the strictly-streaming mode: every run of same-shaped rows is
        // its own bulk copy and nothing is ever held in memory - results must not differ
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) NOT NULL DEFAULT 'def', Value int);
            """);

        var src = new ListSource("Foo", ["Name", "Value"],
            ["a", 1],
            [null, 2],
            ["c", null],
            [null, null]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { MaxBufferedRows = 0 });

        Assert.That((ranges[0].First, ranges[0].Last), Is.EqualTo((1L, 4L)));
        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, CONVERT(nvarchar(20), Value) FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Triple(1, "a", "1"),
            new Triple(2, "def", "2"),
            new Triple(3, "c", null),
            new Triple(4, "def", null),
        }));
    }

    [Test]
    public async Task Insert_InterleavedShapes_TinyBuffer_FlushesInChunks()
    {
        // a source outgrowing the buffer with interleaved shapes flushes chunk by chunk, and a
        // single-shape prefix filling the buffer streams on through the same write - both paths
        // have to preserve values and the identity sequence
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) NOT NULL DEFAULT 'def', Value int);
            """);

        var src = new ListSource("Foo", ["Name", "Value"],
            ["a", 1],
            ["b", 2],
            ["c", 3],       // single-shape prefix longer than the buffer - streams live
            [null, 4],
            ["e", null],
            [null, null],   // interleaved remainder - chunked regrouping
            ["g", 7]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { MaxBufferedRows = 2 });

        Assert.That((ranges[0].First, ranges[0].Last), Is.EqualTo((1L, 7L)));
        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, CONVERT(nvarchar(20), Value) FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Triple(1, "a", "1"),
            new Triple(2, "b", "2"),
            new Triple(3, "c", "3"),
            new Triple(4, "def", "4"),
            new Triple(5, "e", null),
            new Triple(6, "def", null),
            new Triple(7, "g", "7"),
        }));
    }

    [Test]
    public async Task Insert_InterleavedShapes_AllNullRowInBufferedRemainder_InsertsDefaults()
    {
        // the all-null shape has no columns to map, so its buffered rows have to go through
        // INSERT ... DEFAULT VALUES
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Name nvarchar(50) DEFAULT 'def', Tag nvarchar(20) DEFAULT 'tag');
            """);

        var src = new ListSource("Foo", ["Name", "Tag"],
            ["a", null],
            [null, "b"],
            [null, null],
            ["c", "d"]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Pair>("SELECT Name, Tag FROM Foo ORDER BY Name, Tag");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Pair("a", "tag"),
            new Pair("c", "d"),
            new Pair("def", "b"),
            new Pair("def", "tag"),
        }));
    }

    [Test]
    public async Task Insert_InterleavedClasses_SelfReferencingFK_DoesNotDependOnFlushOrder()
    {
        // regrouping flushes rows out of source order; the child of row 4 lands in the first
        // write while row 4 itself - a literal NULL in a defaulted column puts it in the
        // KeepNulls class - lands in the second, which must not fail: bulk copies load
        // unchecked and leave the verification to the deferred constraint pass
        await Connection.ExecAsync("""
            CREATE TABLE Node (Id int PRIMARY KEY, ParentId int NULL FOREIGN KEY REFERENCES Node(Id), Tag nvarchar(20) DEFAULT 'x');
            """);

        var src = new ListSource("Node", ["Id", "ParentId", "Tag"],
            [1, DBNull.Value, "root"],
            [2, 1, null],
            [3, 4, "child-of-4"],
            [4, 1, DBNull.Value]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var tags = await Connection.ReadAsync<string?>("SELECT Tag FROM Node ORDER BY Id");
        Assert.That(tags, Is.EqualTo(new[] { "root", "x", "child-of-4", null }));
        Assert.That(
            (await Connection.ReadAsync<int>("SELECT CONVERT(int, is_not_trusted) FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID('Node')")).Single(),
            Is.Zero, "the deferred pass re-checked and re-trusted the FK");
    }

    [Test]
    public async Task Insert_DBNull_NotNullColumnWithDefault_AppliesDefault()
    {
        // NULL is not storable there, so the only meaningful reading of DBNull is the default -
        // this is also what keeps CSV files (which can only say DBNull) importable
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Status nvarchar(20) NOT NULL DEFAULT 'active')");

        var src = new ListSource("Foo", ["Id", "Status"],
            [1, DBNull.Value],
            [2, "explicit"]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var statuses = await Connection.ReadAsync<string>("SELECT Status FROM Foo ORDER BY Id");
        Assert.That(statuses, Is.EqualTo(new[] { "active", "explicit" }));
    }

    [Test]
    public async Task Merge_XmlColumn_Works()
    {
        // the staging buffer must not force comparability on nullable columns - xml has no '='
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Body xml NULL)");

        var src = new ListSource("Foo", ["Id", "Body"],
            [1, "<a/>"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var body = (await Connection.ReadAsync<string>("SELECT CONVERT(nvarchar(max), Body) FROM Foo")).Single();
        Assert.That(body, Is.EqualTo("<a/>"));
    }

    [Test]
    public async Task Upsert_MatchIndexWithIncludedColumns_StillMatches()
    {
        // INCLUDE columns are not key columns and must not disqualify the index from matching
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int NOT NULL, Payload int NULL, Name nvarchar(50));
            CREATE UNIQUE INDEX UQ_Foo ON Foo (Id) INCLUDE (Payload);
            INSERT Foo VALUES (1, 5, 'old');
            """);

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, "updated"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "updated") }));
    }

    [Test]
    public async Task Upsert_NoMatchableIndex_InsertsEverything()
    {
        // the source misses part of every index key, so nothing can match and all rows insert
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int NOT NULL, Seq int NOT NULL DEFAULT 2, Name nvarchar(50),
                CONSTRAINT PK_Foo PRIMARY KEY (Id, Seq));
            INSERT Foo (Id, Seq, Name) VALUES (1, 1, 'old');
            """);

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, "added"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var names = await Connection.ReadAsync<string>("SELECT Name FROM Foo ORDER BY Seq");
        Assert.That(names, Is.EqualTo(new[] { "old", "added" }));
    }

    [Test]
    public async Task Upsert_TableWithoutIndexes_InsertsEverything()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int NULL, Name nvarchar(50) NULL)");

        var src = new ListSource("Foo", ["Id", "Name"],
            [1, "a"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var names = await Connection.ReadAsync<string>("SELECT Name FROM Foo");
        Assert.That(names, Is.EqualTo(new[] { "a" }));
    }

    [Test]
    public async Task Upsert_SourceWithoutColumns_InsertsDefaultRows()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", [], [], []) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var names = await Connection.ReadAsync<string>("SELECT Name FROM Foo");
        Assert.That(names, Is.EqualTo(new[] { "def", "def" }));
    }

    [Test]
    public async Task Upsert_EmptySourceWithoutColumns_ImportsNothing()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", []) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(), Is.Zero);
    }

    [Test]
    public async Task Insert_SourceReusingRowArray_BuffersCopy()
    {
        // a source may legally reuse one row array across yields - buffered rows must be copies
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, N int)");

        var reused = new object?[2];
        async IAsyncEnumerable<object?[]> Rows()
        {
            await Task.CompletedTask;
            for (int i = 0; i < 5; i++)
            {
                reused[0] = i + 1;
                reused[1] = (i + 1) * 10;
                yield return reused;
            }
        }

        await Service.BulkImportAsync(Connection, AsAsync(new DelegateSource("Foo", ["Id", "N"], Rows)));

        var rows = await Connection.ReadAsync<Row2>("SELECT Id, N FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Row2(1, 10), new Row2(2, 20), new Row2(3, 30), new Row2(4, 40), new Row2(5, 50),
        }));
    }

    [Test]
    public async Task SortedBy_LeadingHintColumnOmitted_SuffixHintDropped()
    {
        // rows sorted by (A, B) are not sorted by B alone - a run that unmaps A must not keep
        // the B hint, or the server rejects the load as incorrectly sorted
        await Connection.ExecAsync("""
            CREATE TABLE Foo (A int NOT NULL DEFAULT 1, B int NOT NULL, C nvarchar(10),
                CONSTRAINT PK_Foo PRIMARY KEY CLUSTERED (A, B));
            """);

        var src = new ListSource("Foo", ["A", "B", "C"],
            [2, 1, "x"],
            [2, 2, "y"],
            [null, 5, "p"],
            [null, 3, "q"])
        {
            SortedBy = [new SqlBulkCopyColumnOrderHint("A", SortOrder.Ascending), new SqlBulkCopyColumnOrderHint("B", SortOrder.Ascending)],
        };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(), Is.EqualTo(4));
    }

    [Test]
    public async Task Insert_DefaultColumn_ValueNullAndDBNull_EachLandsCorrectly()
    {
        // the three meanings a cell can have in one defaulted column, interleaved in one source
        await Connection.ExecAsync("CREATE TABLE Foo (Id int PRIMARY KEY, Status nvarchar(20) DEFAULT 'active')");

        var src = new ListSource("Foo", ["Id", "Status"],
            [1, "explicit"],
            [2, DBNull.Value],
            [3, null],
            [4, DBNull.Value],
            [5, "also-explicit"]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var statuses = await Connection.ReadAsync<string?>("SELECT Status FROM Foo ORDER BY Id");
        Assert.That(statuses, Is.EqualTo(new[] { "explicit", null, "active", null, "also-explicit" }));
    }

    [Test]
    public async Task Insert_NullIdentityCell_ServerAssigns()
    {
        // a null identity cell cannot ride as DBNull under KeepIdentity, so the identity column
        // leaves the mapping for those rows and the server hands out the values
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");

        var src = new ListSource("Foo", ["Id", "Name"],
            [null, "a"],
            [10, "b"],
            [null, "c"]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "a"), new Row(2, "c"), new Row(10, "b") }));
    }

    [Test]
    public async Task Reference_ToServerAssignedIdentityRow_Throws()
    {
        // the import never sees a server-assigned key, so referencing that row must fail loudly
        // instead of resolving to NULL
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE Child  (Id int IDENTITY(1,1) PRIMARY KEY, ParentId int NOT NULL FOREIGN KEY REFERENCES Parent(Id), Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Id", "Name"],
            [null, "server-assigned"],
            [10, "supplied"]);
        var child = new ListSource("Child", ["ParentId", "Tag"],
            [new BulkImportSourceReference(parent, 0), "c-of-server-assigned"]);

        Assert.That(
            async () => await Service.BulkImportAsync(Connection, AsAsync(parent, child)),
            Throws.InvalidOperationException.With.Message.Contains("server"));
    }

    [Test]
    public async Task Reference_ToSuppliedRow_NextToServerAssignedRows_Resolves()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50));
            CREATE TABLE Child  (Id int IDENTITY(1,1) PRIMARY KEY, ParentId int NOT NULL FOREIGN KEY REFERENCES Parent(Id), Tag nvarchar(50));
            """);

        var parent = new ListSource("Parent", ["Id", "Name"],
            [null, "server-assigned"],
            [10, "supplied"]);
        var child = new ListSource("Child", ["ParentId", "Tag"],
            [new BulkImportSourceReference(parent, 1), "c-of-supplied"]);

        await Service.BulkImportAsync(Connection, AsAsync(parent, child));

        Assert.That(
            (await Connection.ReadAsync<int>("SELECT ParentId FROM Child")).Single(),
            Is.EqualTo(10));
    }

    [Test]
    public async Task Insert_OnlyIdentityColumn_AllNull_InsertsDefaultValuesRows()
    {
        // the one row left that maps nothing: identity omitted and no other columns declared -
        // empty mappings would silently fall back to ordinal mapping, so these rows go through
        // INSERT ... DEFAULT VALUES
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) DEFAULT 'def')");

        var src = new ListSource("Foo", ["Id"], [null], [null]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[] { new Row(1, "def"), new Row(2, "def") }));
    }

    [Test]
    public async Task Insert_AllCellsNull_InsertsDefaults()
    {
        // all-null rows ride the KeepNulls-off write like any others - every column mapped, the
        // DBNulls on the wire asking the server for the defaults
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Name nvarchar(50) DEFAULT 'def', Tag nvarchar(20) DEFAULT 'tag');
            """);

        var src = new ListSource("Foo", ["Name", "Tag"],
            [null, null],
            [null, null],
            ["x", null]);

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var names = await Connection.ReadAsync<string>("SELECT Name FROM Foo ORDER BY Name");
        Assert.That(names, Is.EqualTo(new[] { "def", "def", "x" }));
        Assert.That(
            (await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo WHERE Tag = 'tag'")).Single(),
            Is.EqualTo(3));
    }

    [Test]
    public async Task Merge_MixedRowShapes_InsertTakesDefaults_UpdateLeavesOmittedUntouched()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50) DEFAULT 'def', Tag nvarchar(20) DEFAULT 'tag');
            INSERT Foo VALUES (1, 'old', 'old-tag');
            """);

        var src = new ListSource("Foo", ["Id", "Name", "Tag"],
            [1, "updated", null],
            [2, null, "new-tag"],
            [3, null, null]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        var rows = await Connection.ReadAsync<Triple>("SELECT Id, Name, Tag FROM Foo ORDER BY Id");
        Assert.That(rows, Is.EqualTo(new[]
        {
            new Triple(1, "updated", "old-tag"),
            new Triple(2, "def", "new-tag"),
            new Triple(3, "def", "tag"),
        }));
    }

    [Test]
    public async Task SortedBy_DeclaredOrder_LoadsSorted()
    {
        // clustered key is not the identity, so nothing is inferred - the source has to say so
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Code int NOT NULL, Name nvarchar(50),
                CONSTRAINT PK_Foo PRIMARY KEY CLUSTERED (Code));
            """);

        var src = new ListSource("Foo", ["Code", "Name"], [1, "a"], [2, "b"], [3, "c"])
        {
            SortedBy = [new SqlBulkCopyColumnOrderHint("Code", SortOrder.Ascending)],
        };

        await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(await Connection.ReadAsync<Row>("SELECT Code, Name FROM Foo ORDER BY Code"),
            Is.EqualTo(new[] { new Row(1, "a"), new Row(2, "b"), new Row(3, "c") }));
    }

    [Test]
    public async Task SortedBy_OrderTheRowsAreNotIn_Throws()
    {
        // proves the hint reaches the server rather than being quietly dropped
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Code int NOT NULL, Name nvarchar(50),
                CONSTRAINT PK_Foo PRIMARY KEY CLUSTERED (Code));
            """);

        var src = new ListSource("Foo", ["Code", "Name"], [3, "c"], [1, "a"], [2, "b"])
        {
            SortedBy = [new SqlBulkCopyColumnOrderHint("Code", SortOrder.Ascending)],
        };

        Assert.That(
            async () => await Service.BulkImportAsync(Connection, AsAsync(src)),
            Throws.InstanceOf<SqlException>());
    }

    [Test]
    public async Task IndexStrategy_Rebuild_ReenablesIndexesAndKeepsData()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50), Value int);
            CREATE NONCLUSTERED INDEX IX_Foo_Name ON Foo (Name);
            CREATE NONCLUSTERED INDEX IX_Foo_Value ON Foo (Value);
            """);

        var rows = Enumerable.Range(0, 500).Select(i => new object[] { $"n{i}", i }).ToArray();
        var src = new ListSource("Foo", ["Name", "Value"], rows);

        await Service.BulkImportAsync(Connection, AsAsync(src),
            new BulkImportOptions { IndexStrategy = BulkImportIndexStrategy.Rebuild });

        Assert.That(
            await Connection.ReadAsync<int>("SELECT CONVERT(int, is_disabled) FROM sys.indexes WHERE object_id = OBJECT_ID('Foo') AND index_id > 0"),
            Is.All.Zero, "every index is back in service");
        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(), Is.EqualTo(500));
        // read through the rebuilt index to prove its contents match the table
        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo WITH (INDEX(IX_Foo_Name)) WHERE Name LIKE 'n1%'")).Single(),
            Is.EqualTo(111));
    }

    [Test]
    public async Task IndexStrategy_Rebuild_DryRun_LeavesIndexesEnabled()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50));
            CREATE NONCLUSTERED INDEX IX_Foo_Name ON Foo (Name);
            """);

        var src = new ListSource("Foo", ["Id", "Name"], [1, "a"]);

        await Service.BulkImportAsync(Connection, AsAsync(src),
            new BulkImportOptions { IndexStrategy = BulkImportIndexStrategy.Rebuild, DryRun = true });

        Assert.That(
            (await Connection.ReadAsync<int>("SELECT CONVERT(int, is_disabled) FROM sys.indexes WHERE name = 'IX_Foo_Name'")).Single(),
            Is.Zero, "rollback puts the index back");
        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(), Is.Zero);
    }

    [Test]
    public async Task IndexStrategy_Rebuild_LeavesKeyAndForeignKeyIndexesAlone()
    {
        // a unique constraint's index and one a foreign key points at cannot be disabled;
        // touching either would fail the import outright
        await Connection.ExecAsync("""
            CREATE TABLE Parent (Id int PRIMARY KEY, Code int NOT NULL CONSTRAINT UQ_Parent_Code UNIQUE);
            CREATE TABLE Child (Id int PRIMARY KEY, ParentCode int NOT NULL
                CONSTRAINT FK_Child_Parent FOREIGN KEY REFERENCES Parent(Code));
            CREATE NONCLUSTERED INDEX IX_Child_ParentCode ON Child (ParentCode);
            INSERT Parent VALUES (1, 42);
            """);

        var src = new ListSource("Child", ["Id", "ParentCode"], [1, 42]);

        await Service.BulkImportAsync(Connection, AsAsync(src),
            new BulkImportOptions { IndexStrategy = BulkImportIndexStrategy.Rebuild });

        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Child")).Single(), Is.EqualTo(1));
        Assert.That(
            await Connection.ReadAsync<int>("SELECT CONVERT(int, is_disabled) FROM sys.indexes WHERE object_id IN (OBJECT_ID('Child'), OBJECT_ID('Parent')) AND index_id > 0"),
            Is.All.Zero);
    }

    [Test]
    public async Task SchemalessName_QualifiedAndBracketed_ResolveToSameTable()
    {
        await Connection.ExecAsync("CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50))");

        var bare = new ListSource("Foo", ["Name"], ["a"]);
        var qualified = new ListSource("dbo.Foo", ["Name"], ["b"]);
        var bracketed = new ListSource("[dbo].[Foo]", ["Name"], ["c"]);

        var ranges = await Service.BulkImportAsync(Connection, AsAsync(bare, qualified, bracketed));

        Assert.That(ranges.Select(r => r.First), Is.EqualTo(new[] { 1L, 2L, 3L }),
            "all three names share one identity counter");
    }

    [Test]
    public async Task Insert_DescendingClusteredIdentity_LoadsWithoutClaimingOrder()
    {
        // synthesized values ascend, the clustered key descends - asserting a sort order the rows
        // don't have fails the bulk copy outright, so the hint has to stay off here
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) NOT NULL, Name nvarchar(50),
                CONSTRAINT PK_Foo PRIMARY KEY CLUSTERED (Id DESC));
            """);

        var src = new ListSource("Foo", ["Name"], ["a"], ["b"], ["c"]);
        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges[0].First, Is.EqualTo(1));
        Assert.That(await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id"),
            Is.EqualTo(new[] { new Row(1, "a"), new Row(2, "b"), new Row(3, "c") }));
    }

    [Test]
    public async Task Insert_ClusteredKeyIsNotTheIdentity_LoadsWithoutClaimingOrder()
    {
        // rows are sorted on the identity column, which says nothing about the clustered key
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) NOT NULL PRIMARY KEY NONCLUSTERED, Sort nvarchar(10) NOT NULL);
            CREATE CLUSTERED INDEX IX_Foo_Sort ON Foo (Sort);
            """);

        var src = new ListSource("Foo", ["Sort"], ["c"], ["a"], ["b"]);
        var ranges = await Service.BulkImportAsync(Connection, AsAsync(src));

        Assert.That(ranges[0].Last, Is.EqualTo(3));
        Assert.That((await Connection.ReadAsync<int>("SELECT COUNT(*) FROM Foo")).Single(), Is.EqualTo(3));
    }

    [Test]
    public async Task MaxGrantPercent_InsertIgnore_StillInserts()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int IDENTITY(1,1) PRIMARY KEY, Name nvarchar(50) UNIQUE);
            INSERT Foo (Name) VALUES ('a');
            """);

        var src = new ListSource("Foo", ["Name"], ["a"], ["b"]) { Strategy = BulkImportStrategy.InsertIgnore };

        await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { MaxGrantPercent = 2 });

        Assert.That(await Connection.ReadAsync<string>("SELECT Name FROM Foo ORDER BY Name"),
            Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task MaxGrantPercent_Merge_StillUpserts()
    {
        await Connection.ExecAsync("""
            CREATE TABLE Foo (Id int PRIMARY KEY, Name nvarchar(50));
            INSERT Foo VALUES (1, 'old'), (2, 'old');
            """);

        var src = new ListSource("Foo", ["Id", "Name"],
            [2, "updated"],
            [3, "new"]) { Strategy = BulkImportStrategy.Upsert };

        await Service.BulkImportAsync(Connection, AsAsync(src), new BulkImportOptions { MaxGrantPercent = 2 });

        Assert.That(await Connection.ReadAsync<Row>("SELECT Id, Name FROM Foo ORDER BY Id"),
            Is.EqualTo(new[] { new Row(1, "old"), new Row(2, "updated"), new Row(3, "new") }));
    }

    private record Row(int Id, string Name);
    private record Row2(int Id, int N);
    private record Triple(int Id, string? Name, string? Tag);
    private record Pair(string Left, string Right);
    private record BigIntRow(long Id, string Name);
}
