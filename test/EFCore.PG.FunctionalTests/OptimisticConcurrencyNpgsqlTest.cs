using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Xunit;
using Microsoft.EntityFrameworkCore.Storage;

namespace Npgsql.EntityFrameworkCore.PostgreSQL
{
    public class OptimisticConcurrencyNpgsqlTest : OptimisticConcurrencyTestBase<F1UIntWithXminNpgsqlFixture, uint?>
    {
        public OptimisticConcurrencyNpgsqlTest(F1UIntWithXminNpgsqlFixture fixture) : base(fixture) {}

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public async Task Modifying_concurrency_token_only_is_noop()
        {
            using var c = CreateF1Context();

            await c.Database.CreateExecutionStrategy().ExecuteAsync(c, async context =>
            {
                using var transaction = context.Database.BeginTransaction();
                var driver = context.Drivers.Single(d => d.CarNumber == 1);

                Assert.NotEqual(1u, context.Entry(driver).Property<uint>("xmin").CurrentValue);

                driver.Podiums = StorePodiums;
                var firstVersion = context.Entry(driver).Property<uint>("xmin").CurrentValue;
                await context.SaveChangesAsync();

                using var innerContext = CreateF1Context();
                innerContext.Database.UseTransaction(transaction.GetDbTransaction());
                driver = innerContext.Drivers.Single(d => d.CarNumber == 1);

                Assert.NotEqual(firstVersion, innerContext.Entry(driver).Property<uint>("xmin").CurrentValue);
                Assert.Equal(StorePodiums, driver.Podiums);

                var secondVersion = innerContext.Entry(driver).Property<uint>("xmin").CurrentValue;
                innerContext.Entry(driver).Property<uint>("xmin").CurrentValue = firstVersion;
                await innerContext.SaveChangesAsync();

                using var validationContext = CreateF1Context();
                validationContext.Database.UseTransaction(transaction.GetDbTransaction());
                driver = validationContext.Drivers.Single(d => d.CarNumber == 1);

                Assert.Equal(secondVersion, validationContext.Entry(driver).Property<uint>("xmin").CurrentValue);
                Assert.Equal(StorePodiums, driver.Podiums);
            });
        }

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override void External_model_builder_uses_validation() => base.External_model_builder_uses_validation();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override void Nullable_client_side_concurrency_token_can_be_used() => base.Nullable_client_side_concurrency_token_can_be_used();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Simple_concurrency_exception_can_be_resolved_with_client_values() => base.Simple_concurrency_exception_can_be_resolved_with_client_values();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Simple_concurrency_exception_can_be_resolved_with_store_values() => base.Simple_concurrency_exception_can_be_resolved_with_store_values();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Simple_concurrency_exception_can_be_resolved_with_new_values() => base.Simple_concurrency_exception_can_be_resolved_with_new_values();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Simple_concurrency_exception_can_be_resolved_with_store_values_using_equivalent_of_accept_changes() => base.Simple_concurrency_exception_can_be_resolved_with_store_values_using_equivalent_of_accept_changes();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Simple_concurrency_exception_can_be_resolved_with_store_values_using_Reload() => base.Simple_concurrency_exception_can_be_resolved_with_store_values_using_Reload();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Two_concurrency_issues_in_one_to_one_related_entities_can_be_handled_by_dealing_with_dependent_first() => base.Two_concurrency_issues_in_one_to_one_related_entities_can_be_handled_by_dealing_with_dependent_first();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Two_concurrency_issues_in_one_to_many_related_entities_can_be_handled_by_dealing_with_dependent_first() => base.Two_concurrency_issues_in_one_to_many_related_entities_can_be_handled_by_dealing_with_dependent_first();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Concurrency_issue_where_the_FK_is_the_concurrency_token_can_be_handled() => base.Concurrency_issue_where_the_FK_is_the_concurrency_token_can_be_handled();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Change_in_independent_association_results_in_independent_association_exception() => base.Change_in_independent_association_results_in_independent_association_exception();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Change_in_independent_association_after_change_in_different_concurrency_token_results_in_independent_association_exception() => base.Change_in_independent_association_after_change_in_different_concurrency_token_results_in_independent_association_exception();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Attempting_to_delete_same_relationship_twice_for_many_to_many_results_in_independent_association_exception() => base.Attempting_to_delete_same_relationship_twice_for_many_to_many_results_in_independent_association_exception();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Attempting_to_add_same_relationship_twice_for_many_to_many_results_in_independent_association_exception() => base.Attempting_to_add_same_relationship_twice_for_many_to_many_results_in_independent_association_exception();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Concurrency_issue_where_a_complex_type_nested_member_is_the_concurrency_token_can_be_handled() => base.Concurrency_issue_where_a_complex_type_nested_member_is_the_concurrency_token_can_be_handled();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Adding_the_same_entity_twice_results_in_DbUpdateException() => base.Adding_the_same_entity_twice_results_in_DbUpdateException();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Deleting_the_same_entity_twice_results_in_DbUpdateConcurrencyException() => base.Deleting_the_same_entity_twice_results_in_DbUpdateConcurrencyException();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Updating_then_deleting_the_same_entity_results_in_DbUpdateConcurrencyException() => base.Updating_then_deleting_the_same_entity_results_in_DbUpdateConcurrencyException();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Updating_then_deleting_the_same_entity_results_in_DbUpdateConcurrencyException_which_can_be_resolved_with_store_values() => base.Updating_then_deleting_the_same_entity_results_in_DbUpdateConcurrencyException_which_can_be_resolved_with_store_values();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Deleting_then_updating_the_same_entity_results_in_DbUpdateConcurrencyException() => base.Deleting_then_updating_the_same_entity_results_in_DbUpdateConcurrencyException();

        [Fact(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Deleting_then_updating_the_same_entity_results_in_DbUpdateConcurrencyException_which_can_be_resolved_with_store_values() => base.Deleting_then_updating_the_same_entity_results_in_DbUpdateConcurrencyException_which_can_be_resolved_with_store_values();

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_an_Added_entity_that_is_not_in_database_is_no_op(bool async) => base.Calling_Reload_on_an_Added_entity_that_is_not_in_database_is_no_op(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_an_Unchanged_entity_that_is_not_in_database_detaches_it(bool async) => base.Calling_Reload_on_an_Unchanged_entity_that_is_not_in_database_detaches_it(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_a_Modified_entity_that_is_not_in_database_detaches_it(bool async) => base.Calling_Reload_on_a_Modified_entity_that_is_not_in_database_detaches_it(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_a_Deleted_entity_that_is_not_in_database_detaches_it(bool async) => base.Calling_Reload_on_a_Deleted_entity_that_is_not_in_database_detaches_it(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_a_Detached_entity_that_is_not_in_database_detaches_it(bool async) => base.Calling_Reload_on_a_Detached_entity_that_is_not_in_database_detaches_it(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_an_Unchanged_entity_makes_the_entity_unchanged(bool async) => base.Calling_Reload_on_an_Unchanged_entity_makes_the_entity_unchanged(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_a_Modified_entity_makes_the_entity_unchanged(bool async) => base.Calling_Reload_on_a_Modified_entity_makes_the_entity_unchanged(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_a_Deleted_entity_makes_the_entity_unchanged(bool async) => base.Calling_Reload_on_a_Deleted_entity_makes_the_entity_unchanged(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_an_Added_entity_that_was_saved_elsewhere_makes_the_entity_unchanged(bool async) => base.Calling_Reload_on_an_Added_entity_that_was_saved_elsewhere_makes_the_entity_unchanged(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_a_Detached_entity_makes_the_entity_unchanged(bool async) => base.Calling_Reload_on_a_Detached_entity_makes_the_entity_unchanged(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_GetDatabaseValues_on_owned_entity_works(bool async) => base.Calling_GetDatabaseValues_on_owned_entity_works(async);

        [ConditionalTheory(Skip = "https://github.com/verygoodsoftwareorg/cockroach-efcore/issues/9")]
        public override Task Calling_Reload_on_owned_entity_works(bool async) => base.Calling_Reload_on_owned_entity_works(async);

        protected override void UseTransaction(DatabaseFacade facade, IDbContextTransaction transaction)
            => facade.UseTransaction(transaction.GetDbTransaction());
    }
}
