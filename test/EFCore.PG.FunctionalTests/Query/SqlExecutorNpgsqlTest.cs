using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query
{
    public class SqlExecutorNpgsqlTest : SqlExecutorTestBase<NorthwindQueryNpgsqlFixture<NoopModelCustomizer>>
    {
        public SqlExecutorNpgsqlTest(NorthwindQueryNpgsqlFixture<NoopModelCustomizer> fixture)
            : base(fixture)
        {
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Executes_stored_procedure() => base.Executes_stored_procedure();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Executes_stored_procedure_with_parameter() => base.Executes_stored_procedure_with_parameter();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Executes_stored_procedure_with_generated_parameter() => base.Executes_stored_procedure_with_generated_parameter();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override Task Executes_stored_procedure_async() => base.Executes_stored_procedure_async();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override Task Executes_stored_procedure_with_parameter_async() => base.Executes_stored_procedure_with_parameter_async();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override Task Executes_stored_procedure_with_generated_parameter_async() => base.Executes_stored_procedure_with_generated_parameter_async();

        protected override DbParameter CreateDbParameter(string name, object value)
            => new NpgsqlParameter
            {
                ParameterName = name,
                Value = value
            };

        protected override string TenMostExpensiveProductsSproc => @"SELECT * FROM ""Ten Most Expensive Products""()";

        protected override string CustomerOrderHistorySproc => @"SELECT * FROM ""CustOrderHist""(@CustomerID)";

        protected override string CustomerOrderHistoryWithGeneratedParameterSproc => @"SELECT * FROM ""CustOrderHist""({0})";
    }
}
