﻿using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Npgsql.EntityFrameworkCore.PostgreSQL.TestUtilities;
using Xunit;
using Xunit.Abstractions;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.Query
{
    public class UdfDbFunctionNpgsqlTests : UdfDbFunctionTestBase<UdfDbFunctionNpgsqlTests.UdfNpgsqlFixture>
    {
        // ReSharper disable once UnusedParameter.Local
        public UdfDbFunctionNpgsqlTests(UdfNpgsqlFixture fixture, ITestOutputHelper testOutputHelper)
            : base(fixture)
        {
            Fixture.TestSqlLoggerFactory.Clear();
            //Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
        }

        #region Static

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Extension_Method_Static()
        {
            base.Scalar_Function_Extension_Method_Static();

            AssertSql(
                @"SELECT COUNT(*)::INT
FROM ""Customers"" AS c
WHERE ""IsDate""(c.""FirstName"") = FALSE");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_With_Translator_Translates_Static()
        {
            base.Scalar_Function_With_Translator_Translates_Static();

            AssertSql(
                @"@__customerId_0='3'

SELECT length(c.""LastName"")
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_ClientEval_Method_As_Translateable_Method_Parameter_Static()
            => base.Scalar_Function_ClientEval_Method_As_Translateable_Method_Parameter_Static();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Constant_Parameter_Static()
        {
            base.Scalar_Function_Constant_Parameter_Static();

            AssertSql(
                @"@__customerId_0='1'

SELECT ""CustomerOrderCount""(@__customerId_0)
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Correlated_Static()
        {
            base.Scalar_Function_Anonymous_Type_Select_Correlated_Static();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(c.""Id"") AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 1
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Not_Correlated_Static()
        {
            base.Scalar_Function_Anonymous_Type_Select_Not_Correlated_Static();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(1) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 1
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Parameter_Static()
        {
            base.Scalar_Function_Anonymous_Type_Select_Parameter_Static();

            AssertSql(
                @"@__customerId_0='1'

SELECT c.""LastName"", ""CustomerOrderCount""(@__customerId_0) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Nested_Static()
        {
            base.Scalar_Function_Anonymous_Type_Select_Nested_Static();

            AssertSql(
                @"@__starCount_1='3'
@__customerId_0='3'

SELECT c.""LastName"", ""StarValue""(@__starCount_1, ""CustomerOrderCount""(@__customerId_0)) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Correlated_Static()
        {
            base.Scalar_Function_Where_Correlated_Static();

            AssertSql(
                @"SELECT lower(CAST(c.""Id"" AS text))
FROM ""Customers"" AS c
WHERE ""IsTopCustomer""(c.""Id"")");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Not_Correlated_Static()
        {
            base.Scalar_Function_Where_Not_Correlated_Static();

            AssertSql(
                @"@__startDate_0='2000-04-01T00:00:00.0000000' (Nullable = true) (DbType = DateTime)

SELECT c.""Id""
FROM ""Customers"" AS c
WHERE ""GetCustomerWithMostOrdersAfterDate""(@__startDate_0) = c.""Id""
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Parameter_Static()
        {
            base.Scalar_Function_Where_Parameter_Static();

            AssertSql(
                @"@__period_0='0'

SELECT c.""Id""
FROM ""Customers"" AS c
WHERE c.""Id"" = ""GetCustomerWithMostOrdersAfterDate""(""GetReportingPeriodStartDate""(@__period_0))
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Nested_Static()
        {
            base.Scalar_Function_Where_Nested_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
WHERE c.""Id"" = ""GetCustomerWithMostOrdersAfterDate""(""GetReportingPeriodStartDate""(0))
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Correlated_Static()
        {
            base.Scalar_Function_Let_Correlated_Static();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(c.""Id"") AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 2
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Not_Correlated_Static()
        {
            base.Scalar_Function_Let_Not_Correlated_Static();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(2) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 2
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Not_Parameter_Static()
        {
            base.Scalar_Function_Let_Not_Parameter_Static();

            AssertSql(
                @"@__customerId_0='2'

SELECT c.""LastName"", ""CustomerOrderCount""(@__customerId_0) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Nested_Static()
        {
            base.Scalar_Function_Let_Nested_Static();

            AssertSql(
                @"@__starCount_0='3'
@__customerId_1='1'

SELECT c.""LastName"", ""StarValue""(@__starCount_0, ""CustomerOrderCount""(@__customerId_1)) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_1
LIMIT 2");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == AddOneStatic([c].Id))'")]
        public override void Scalar_Nested_Function_Unwind_Client_Eval_Where_Static()
        {
            base.Scalar_Nested_Function_Unwind_Client_Eval_Where_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'orderby AddOneStatic([c].Id) asc'")]
        public override void Scalar_Nested_Function_Unwind_Client_Eval_OrderBy_Static()
        {
            base.Scalar_Nested_Function_Unwind_Client_Eval_OrderBy_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Nested_Function_Unwind_Client_Eval_Select_Static()
        {
            base.Scalar_Nested_Function_Unwind_Client_Eval_Select_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
ORDER BY c.""Id""");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == AddOneStatic(Abs(CustomerOrderCountWithClientStatic([c].Id))))'")]
        public override void Scalar_Nested_Function_Client_BCL_UDF_Static()
        {
            base.Scalar_Nested_Function_Client_BCL_UDF_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == AddOneStatic(CustomerOrderCountWithClientStatic(Abs([c].Id))))'")]
        public override void Scalar_Nested_Function_Client_UDF_BCL_Static()
        {
            base.Scalar_Nested_Function_Client_UDF_BCL_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == Abs(AddOneStatic(CustomerOrderCountWithClientStatic([c].Id))))'")]
        public override void Scalar_Nested_Function_BCL_Client_UDF_Static()
        {
            base.Scalar_Nested_Function_BCL_Client_UDF_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (1 == Abs(CustomerOrderCountWithClientStatic(AddOneStatic([c].Id))))'")]
        public override void Scalar_Nested_Function_BCL_UDF_Client_Static()
        {
            base.Scalar_Nested_Function_BCL_UDF_Client_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (1 == CustomerOrderCountWithClientStatic(Abs(AddOneStatic([c].Id))))'")]
        public override void Scalar_Nested_Function_UDF_BCL_Client_Static()
        {
            base.Scalar_Nested_Function_UDF_BCL_Client_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (1 == CustomerOrderCountWithClientStatic(AddOneStatic(Abs([c].Id))))'")]
        public override void Scalar_Nested_Function_UDF_Client_BCL_Static()
        {
            base.Scalar_Nested_Function_UDF_Client_BCL_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (3 == AddOneStatic(Abs([c].Id)))'")]
        public override void Scalar_Nested_Function_Client_BCL_Static()
        {
            base.Scalar_Nested_Function_Client_BCL_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == AddOneStatic(CustomerOrderCountWithClientStatic([c].Id)))'")]
        public override void Scalar_Nested_Function_Client_UDF_Static()
        {
            base.Scalar_Nested_Function_Client_UDF_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (3 == Abs(AddOneStatic([c].Id)))'")]
        public override void Scalar_Nested_Function_BCL_Client_Static()
        {
            base.Scalar_Nested_Function_BCL_Client_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Nested_Function_BCL_UDF_Static()
        {
            base.Scalar_Nested_Function_BCL_UDF_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
WHERE 3 = abs(""CustomerOrderCount""(c.""Id""))
LIMIT 2");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == CustomerOrderCountWithClientStatic(AddOneStatic([c].Id)))'")]
        public override void Scalar_Nested_Function_UDF_Client_Static()
        {
            base.Scalar_Nested_Function_UDF_Client_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Nested_Function_UDF_BCL_Static()
        {
            base.Scalar_Nested_Function_UDF_BCL_Static();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
WHERE 3 = ""CustomerOrderCount""(abs(c.""Id""))
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Nullable_navigation_property_access_preserves_schema_for_sql_function()
        {
            base.Nullable_navigation_property_access_preserves_schema_for_sql_function();

            AssertSql(
                @"SELECT dbo.""IdentityString""(c.""FirstName"")
FROM ""Orders"" AS o
INNER JOIN ""Customers"" AS c ON o.""CustomerId"" = c.""Id""
ORDER BY o.""Id""
LIMIT 1");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Compare_function_without_null_propagation_to_null() => base.Compare_function_without_null_propagation_to_null();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Compare_function_with_null_propagation_to_null() => base.Compare_function_with_null_propagation_to_null();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Compare_non_nullable_function_to_null_gets_optimized() => base.Compare_non_nullable_function_to_null_gets_optimized();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Compare_functions_returning_int_that_take_nullable_param_which_propagates_null() => base.Compare_functions_returning_int_that_take_nullable_param_which_propagates_null();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_SqlFragment_Static() => base.Scalar_Function_SqlFragment_Static();

        #endregion

        #region Instance

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Non_Static()
        {
            base.Scalar_Function_Non_Static();

            AssertSql(
                @"SELECT ""StarValue""(4, c.""Id"") AS ""Id"", ""DollarValue""(2, c.""LastName"") AS ""LastName""
FROM ""Customers"" AS c
WHERE c.""Id"" = 1
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Extension_Method_Instance()
        {
            base.Scalar_Function_Extension_Method_Instance();

            AssertSql(
                @"SELECT COUNT(*)::INT
FROM ""Customers"" AS c
WHERE ""IsDate""(c.""FirstName"") = FALSE");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_With_Translator_Translates_Instance()
        {
            base.Scalar_Function_With_Translator_Translates_Instance();

            AssertSql(
                @"@__customerId_0='3'

SELECT length(c.""LastName"")
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_ClientEval_Method_As_Translateable_Method_Parameter_Instance() => base.Scalar_Function_ClientEval_Method_As_Translateable_Method_Parameter_Instance();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Constant_Parameter_Instance()
        {
            base.Scalar_Function_Constant_Parameter_Instance();

            AssertSql(
                @"@__customerId_1='1'

SELECT ""CustomerOrderCount""(@__customerId_1)
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Correlated_Instance()
        {
            base.Scalar_Function_Anonymous_Type_Select_Correlated_Instance();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(c.""Id"") AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 1
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Not_Correlated_Instance()
        {
            base.Scalar_Function_Anonymous_Type_Select_Not_Correlated_Instance();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(1) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 1
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Parameter_Instance()
        {
            base.Scalar_Function_Anonymous_Type_Select_Parameter_Instance();

            AssertSql(
                @"@__customerId_0='1'

SELECT c.""LastName"", ""CustomerOrderCount""(@__customerId_0) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Anonymous_Type_Select_Nested_Instance()
        {
            base.Scalar_Function_Anonymous_Type_Select_Nested_Instance();

            AssertSql(
                @"@__starCount_2='3'
@__customerId_0='3'

SELECT c.""LastName"", ""StarValue""(@__starCount_2, ""CustomerOrderCount""(@__customerId_0)) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_0
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Correlated_Instance()
        {
            base.Scalar_Function_Where_Correlated_Instance();

            AssertSql(
                @"SELECT lower(CAST(c.""Id"" AS text))
FROM ""Customers"" AS c
WHERE ""IsTopCustomer""(c.""Id"")");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Not_Correlated_Instance()
        {
            base.Scalar_Function_Where_Not_Correlated_Instance();

            AssertSql(
                @"@__startDate_1='2000-04-01T00:00:00.0000000' (Nullable = true) (DbType = DateTime)

SELECT c.""Id""
FROM ""Customers"" AS c
WHERE ""GetCustomerWithMostOrdersAfterDate""(@__startDate_1) = c.""Id""
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Parameter_Instance()
        {
            base.Scalar_Function_Where_Parameter_Instance();

            AssertSql(
                @"@__period_1='0'

SELECT c.""Id""
FROM ""Customers"" AS c
WHERE c.""Id"" = ""GetCustomerWithMostOrdersAfterDate""(""GetReportingPeriodStartDate""(@__period_1))
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Where_Nested_Instance()
        {
            base.Scalar_Function_Where_Nested_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
WHERE c.""Id"" = ""GetCustomerWithMostOrdersAfterDate""(""GetReportingPeriodStartDate""(0))
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Correlated_Instance()
        {
            base.Scalar_Function_Let_Correlated_Instance();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(c.""Id"") AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 2
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Not_Correlated_Instance()
        {
            base.Scalar_Function_Let_Not_Correlated_Instance();

            AssertSql(
                @"SELECT c.""LastName"", ""CustomerOrderCount""(2) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = 2
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Not_Parameter_Instance()
        {
            base.Scalar_Function_Let_Not_Parameter_Instance();

            AssertSql(
                @"@__customerId_1='2'

SELECT c.""LastName"", ""CustomerOrderCount""(@__customerId_1) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_1
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Function_Let_Nested_Instance()
        {
            base.Scalar_Function_Let_Nested_Instance();

            AssertSql(
                @"@__starCount_1='3'
@__customerId_2='1'

SELECT c.""LastName"", ""StarValue""(@__starCount_1, ""CustomerOrderCount""(@__customerId_2)) AS ""OrderCount""
FROM ""Customers"" AS c
WHERE c.""Id"" = @__customerId_2
LIMIT 2");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == __context_0.AddOneInstance([c].Id))'")]
        public override void Scalar_Nested_Function_Unwind_Client_Eval_Where_Instance()
        {
            base.Scalar_Nested_Function_Unwind_Client_Eval_Where_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'orderby __context_0.AddOneInstance([c].Id) asc'")]
        public override void Scalar_Nested_Function_Unwind_Client_Eval_OrderBy_Instance()
        {
            base.Scalar_Nested_Function_Unwind_Client_Eval_OrderBy_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Nested_Function_Unwind_Client_Eval_Select_Instance()
        {
            base.Scalar_Nested_Function_Unwind_Client_Eval_Select_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
ORDER BY c.""Id""");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == __context_0.AddOneInstance(Abs(__context_0.CustomerOrderCountWithClientInstance([c].Id))))'")]
        public override void Scalar_Nested_Function_Client_BCL_UDF_Instance()
        {
            base.Scalar_Nested_Function_Client_BCL_UDF_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == __context_0.AddOneInstance(__context_0.CustomerOrderCountWithClientInstance(Abs([c].Id))))'")]
        public override void Scalar_Nested_Function_Client_UDF_BCL_Instance()
        {
            base.Scalar_Nested_Function_Client_UDF_BCL_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == Abs(__context_0.AddOneInstance(__context_0.CustomerOrderCountWithClientInstance([c].Id))))'")]
        public override void Scalar_Nested_Function_BCL_Client_UDF_Instance()
        {
            base.Scalar_Nested_Function_BCL_Client_UDF_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (1 == Abs(__context_0.CustomerOrderCountWithClientInstance(__context_0.AddOneInstance([c].Id))))'")]
        public override void Scalar_Nested_Function_BCL_UDF_Client_Instance()
        {
            base.Scalar_Nested_Function_BCL_UDF_Client_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (1 == __context_0.CustomerOrderCountWithClientInstance(Abs(__context_0.AddOneInstance([c].Id))))'")]
        public override void Scalar_Nested_Function_UDF_BCL_Client_Instance()
        {
            base.Scalar_Nested_Function_UDF_BCL_Client_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (1 == __context_0.CustomerOrderCountWithClientInstance(__context_0.AddOneInstance(Abs([c].Id))))'")]
        public override void Scalar_Nested_Function_UDF_Client_BCL_Instance()
        {
            base.Scalar_Nested_Function_UDF_Client_BCL_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (3 == __context_0.AddOneInstance(Abs([c].Id)))'")]
        public override void Scalar_Nested_Function_Client_BCL_Instance()
        {
            base.Scalar_Nested_Function_Client_BCL_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == __context_0.AddOneInstance(__context_0.CustomerOrderCountWithClientInstance([c].Id)))'")]
        public override void Scalar_Nested_Function_Client_UDF_Instance()
        {
            base.Scalar_Nested_Function_Client_UDF_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (3 == Abs(__context_0.AddOneInstance([c].Id)))'")]
        public override void Scalar_Nested_Function_BCL_Client_Instance()
        {
            base.Scalar_Nested_Function_BCL_Client_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Nested_Function_BCL_UDF_Instance()
        {
            base.Scalar_Nested_Function_BCL_UDF_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
WHERE 3 = abs(""CustomerOrderCount""(c.""Id""))
LIMIT 2");
        }

        [Fact(Skip = "Issue #14935. Cannot eval 'where (2 == __context_0.CustomerOrderCountWithClientInstance(__context_0.AddOneInstance([c].Id)))'")]
        public override void Scalar_Nested_Function_UDF_Client_Instance()
        {
            base.Scalar_Nested_Function_UDF_Client_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Scalar_Nested_Function_UDF_BCL_Instance()
        {
            base.Scalar_Nested_Function_UDF_BCL_Instance();

            AssertSql(
                @"SELECT c.""Id""
FROM ""Customers"" AS c
WHERE 3 = ""CustomerOrderCount""(abs(c.""Id""))
LIMIT 2");
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Anonymous_Collection_No_PK_Throws() => base.QF_Anonymous_Collection_No_PK_Throws();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Anonymous_Collection_No_IQueryable_In_Projection_Throws() => base.QF_Anonymous_Collection_No_IQueryable_In_Projection_Throws();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Stand_Alone() => base.QF_Stand_Alone();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Stand_Alone_Parameter() => base.QF_Stand_Alone_Parameter();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_CrossApply_Correlated_Select_QF_Type() => base.QF_CrossApply_Correlated_Select_QF_Type();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_CrossApply_Correlated_Select_Anonymous() => base.QF_CrossApply_Correlated_Select_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_Direct_In_Anonymous() => base.QF_Select_Direct_In_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_Correlated_Direct_With_Function_Query_Parameter_Correlated_In_Anonymous() => base.QF_Select_Correlated_Direct_With_Function_Query_Parameter_Correlated_In_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_Correlated_Subquery_In_Anonymous() => base.QF_Select_Correlated_Subquery_In_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_Correlated_Subquery_In_Anonymous_Nested_With_QF() => base.QF_Select_Correlated_Subquery_In_Anonymous_Nested_With_QF();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_Correlated_Subquery_In_Anonymous_Nested() => base.QF_Select_Correlated_Subquery_In_Anonymous_Nested();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_Correlated_Subquery_In_Anonymous_MultipleCollections() => base.QF_Select_Correlated_Subquery_In_Anonymous_MultipleCollections();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_NonCorrelated_Subquery_In_Anonymous() => base.QF_Select_NonCorrelated_Subquery_In_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Select_NonCorrelated_Subquery_In_Anonymous_Parameter() => base.QF_Select_NonCorrelated_Subquery_In_Anonymous_Parameter();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Correlated_Select_In_Anonymous() => base.QF_Correlated_Select_In_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_CrossApply_Correlated_Select_Result() => base.QF_CrossApply_Correlated_Select_Result();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_CrossJoin_Not_Correlated() => base.QF_CrossJoin_Not_Correlated();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_CrossJoin_Parameter() => base.QF_CrossJoin_Parameter();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Join() => base.QF_Join();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_LeftJoin_Select_Anonymous() => base.QF_LeftJoin_Select_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_LeftJoin_Select_Result() => base.QF_LeftJoin_Select_Result();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_OuterApply_Correlated_Select_QF() => base.QF_OuterApply_Correlated_Select_QF();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_OuterApply_Correlated_Select_Entity() => base.QF_OuterApply_Correlated_Select_Entity();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_OuterApply_Correlated_Select_Anonymous() => base.QF_OuterApply_Correlated_Select_Anonymous();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Nested() => base.QF_Nested();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Correlated_Nested_Func_Call() => base.QF_Correlated_Nested_Func_Call();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void QF_Correlated_Func_Call_With_Navigation() => base.QF_Correlated_Func_Call_With_Navigation();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void DbSet_mapped_to_function() => base.DbSet_mapped_to_function();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void TVF_backing_entity_type_mapped_to_view() => base.TVF_backing_entity_type_mapped_to_view();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Udf_with_argument_being_comparison_to_null_parameter() => base.Udf_with_argument_being_comparison_to_null_parameter();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/27")]
        public override void Udf_with_argument_being_comparison_of_nullable_columns() => base.Udf_with_argument_being_comparison_of_nullable_columns();

        #endregion

        protected class NpgsqlUDFSqlContext : UDFSqlContext
        {
            public NpgsqlUDFSqlContext(DbContextOptions options)
                : base(options)
            {
            }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                base.OnModelCreating(modelBuilder);

                // IsDate is a built-in SQL Server function, that in the base class is mapped as built-in, which means we
                // don't get any quotes. We remap it as non-built-in by including a (null) schema.
                var isDateMethodInfo = typeof(UDFSqlContext).GetMethod(nameof(IsDateStatic));
                modelBuilder.HasDbFunction(isDateMethodInfo)
                    .HasTranslation(args => new SqlFunctionExpression(
                        schema: null,
                        "IsDate",
                        args,
                        nullable: true,
                        argumentsPropagateNullability: args.Select(a => true).ToList(),
                        isDateMethodInfo.ReturnType,
                        typeMapping: null));

                var isDateMethodInfo2 = typeof(UDFSqlContext).GetMethod(nameof(IsDateInstance));
                modelBuilder.HasDbFunction(isDateMethodInfo2)
                    .HasTranslation(args => new SqlFunctionExpression(
                        schema: null,
                        "IsDate",
                        args,
                        nullable: true,
                        argumentsPropagateNullability: args.Select(a => true).ToList(),
                        isDateMethodInfo2.ReturnType,
                        typeMapping: null));

                // Base class maps to len(), but in PostgreSQL it's called length()
                var methodInfo = typeof(UDFSqlContext).GetMethod(nameof(MyCustomLengthStatic));
                modelBuilder.HasDbFunction(methodInfo)
                    .HasTranslation(args => new SqlFunctionExpression(
                        "length",
                        args,
                        nullable: true,
                        argumentsPropagateNullability: args.Select(a => true).ToList(),
                        methodInfo.ReturnType,
                        typeMapping: null));

                var methodInfo2 = typeof(UDFSqlContext).GetMethod(nameof(MyCustomLengthInstance));
                modelBuilder.HasDbFunction(methodInfo2)
                    .HasTranslation(args => new SqlFunctionExpression(
                        "length",
                        args,
                        nullable: true,
                        argumentsPropagateNullability: args.Select(a => true).ToList(),
                        methodInfo2.ReturnType,
                        typeMapping: null));
            }
        }

        public class UdfNpgsqlFixture : UdfFixtureBase
        {
            protected override string StoreName { get; } = "UDFDbFunctionNpgsqlTests";
            protected override ITestStoreFactory TestStoreFactory => NpgsqlTestStoreFactory.Instance;
            protected override Type ContextType { get; } = typeof(NpgsqlUDFSqlContext);

            protected override void Seed(DbContext context)
            {
                base.Seed(context);

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""CustomerOrderCount"" (""customerId"" INTEGER)
                                                    RETURNS INTEGER
                                                    AS $$ SELECT COUNT(""Id"")::INTEGER FROM ""Orders"" WHERE ""CustomerId"" = $1 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""StarValue"" (""starCount"" INTEGER, value TEXT)
                                                    RETURNS TEXT
                                                    AS $$ SELECT repeat('*', $1) || $2 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""StarValue"" (""starCount"" INTEGER, value INTEGER)
                                                    RETURNS TEXT
                                                    AS $$ SELECT repeat('*', $1) || $2 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""DollarValue"" (""starCount"" INTEGER, value TEXT)
                                                    RETURNS TEXT
                                                    AS $$ SELECT repeat('$', $1) || $2 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""GetReportingPeriodStartDate"" (period INTEGER)
                                                    RETURNS DATE
                                                    AS $$ SELECT DATE '1998-01-01' $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""GetCustomerWithMostOrdersAfterDate"" (searchDate TIMESTAMP)
                                                    RETURNS INTEGER
                                                    AS $$ SELECT ""CustomerId""
                                                          FROM ""Orders""
                                                          WHERE ""OrderDate"" > $1
                                                          GROUP BY ""CustomerId""
                                                          ORDER BY COUNT(""Id"") DESC
                                                          LIMIT 1 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""IsTopCustomer"" (""customerId"" INTEGER)
                                                    RETURNS BOOL
                                                    AS $$ SELECT $1 = 1 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE SCHEMA IF NOT EXISTS dbo");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION dbo.""IdentityString"" (""customerName"" TEXT)
                                                    RETURNS TEXT
                                                    AS $$ SELECT $1 $$
                                                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(@"
                    CREATE FUNCTION ""GetCustomerOrderCountByYear""(""customerId"" INT)
                    RETURNS TABLE (""CustomerId"" INT, ""Count"" INT, ""Year"" INT)
                    AS $$
                    SELECT ""CustomerId"", COUNT(""Id"")::INT, EXTRACT(year FROM ""OrderDate"")::INT
                        FROM ""Orders""
                        WHERE ""CustomerId"" = $1
                        GROUP BY ""CustomerId"", EXTRACT(year FROM ""OrderDate"")
                        ORDER BY EXTRACT(year FROM ""OrderDate"")
                    $$ LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(@"
                    CREATE FUNCTION ""StringLength""(""s"" TEXT)
                    RETURNS INT
                    AS $$ SELECT LENGTH($1) $$
                    LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(@"
                    CREATE FUNCTION ""GetCustomerOrderCountByYearOnlyFrom2000""(""customerId"" INT, ""onlyFrom2000"" BOOL)
                    RETURNS TABLE (""CustomerId"" INT, ""Count"" INT, ""Year"" INT)
                    AS $$
                    SELECT $1, COUNT(""Id"")::INT, EXTRACT(year FROM ""OrderDate"")::INT
                        FROM ""Orders""
                        WHERE ""CustomerId"" = 1 AND (NOT $2 OR $2 IS NULL OR ($2 AND EXTRACT(year FROM ""OrderDate"") = 2000))
                        GROUP BY ""CustomerId"", EXTRACT(year FROM ""OrderDate"")
                        ORDER BY EXTRACT(year FROM ""OrderDate"")
                    $$ LANGUAGE SQL");

                 context.Database.ExecuteSqlRaw(@"
                    CREATE FUNCTION ""GetTopTwoSellingProducts""()
                    RETURNS TABLE (""ProductId"" INT, ""AmountSold"" INT)
                    AS $$
                        SELECT ""ProductId"", SUM(""Quantity"")::INT AS ""totalSold""
                        FROM ""LineItem""
                        GROUP BY ""ProductId""
                        ORDER BY ""totalSold"" DESC
                        LIMIT 2
                    $$ LANGUAGE SQL");

                 context.Database.ExecuteSqlRaw(@"
                    CREATE FUNCTION ""GetOrdersWithMultipleProducts""(""customerId"" INT)
                    RETURNS TABLE (""OrderId"" INT, ""CustomerId"" INT, ""OrderDate"" TIMESTAMP)
                    AS $$
                        SELECT o.""Id"", $1, ""OrderDate""
                        FROM ""Orders"" AS o
                        JOIN ""LineItem"" li ON o.""Id"" = li.""OrderId""
                        WHERE o.""CustomerId"" = $1
                        GROUP BY o.""Id"", ""OrderDate""
                        HAVING COUNT(""ProductId"") > 1
                    $$ LANGUAGE SQL");

                 context.Database.ExecuteSqlRaw(@"
                    CREATE FUNCTION ""AddValues"" (a INT, b INT) RETURNS INT
                    AS $$ SELECT $1 + $2 $$ LANGUAGE SQL");

                context.Database.ExecuteSqlRaw(
                    @"CREATE FUNCTION ""IsDate""(s TEXT)
                                                    RETURNS BOOLEAN AS $$
                                                    BEGIN
                                                        PERFORM s::DATE;
                                                        RETURN TRUE;
                                                    EXCEPTION WHEN OTHERS THEN
                                                        RETURN FALSE;
                                                    END;
                                                    $$ LANGUAGE PLPGSQL;");

                context.SaveChanges();
            }
        }

        void AssertSql(params string[] expected)
            => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
    }
}
