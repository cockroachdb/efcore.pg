using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestModels.ConcurrencyModel;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Npgsql.EntityFrameworkCore.PostgreSQL.TestUtilities;

namespace Npgsql.EntityFrameworkCore.PostgreSQL
{
    public class F1UIntNpgsqlFixture : F1NpgsqlFixtureBase<uint?>
    {
        public F1UIntNpgsqlFixture() : base(useXmin: false)
        {

        }
    }
    public class F1UIntWithXminNpgsqlFixture : F1NpgsqlFixtureBase<uint?>
    {
        public F1UIntWithXminNpgsqlFixture() : base(useXmin: true)
        {

        }
    }

    public class F1NpgsqlFixture : F1NpgsqlFixtureBase<byte[]>
    {
        public F1NpgsqlFixture() : base(useXmin: false)
        {

        }
    }

    public abstract class F1NpgsqlFixtureBase<TRowVersion> : F1RelationalFixture<TRowVersion>
    {
        private readonly bool _useXmin;

        protected F1NpgsqlFixtureBase(bool useXmin)
        {
            _useXmin = useXmin;
        }

        protected override ITestStoreFactory TestStoreFactory => NpgsqlTestStoreFactory.Instance;
        public override TestHelpers TestHelpers => NpgsqlTestHelpers.Instance;

        protected override void BuildModelExternal(ModelBuilder modelBuilder)
        {
            base.BuildModelExternal(modelBuilder);

            if (_useXmin)
            {
                modelBuilder.Entity<Chassis>().UseXminAsConcurrencyToken();
                modelBuilder.Entity<Driver>().UseXminAsConcurrencyToken();
                modelBuilder.Entity<Team>().UseXminAsConcurrencyToken();
            }
        }
    }
}
