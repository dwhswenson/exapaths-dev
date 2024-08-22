from exapaths.dag.group_tasks import OPSTaskGrouper, StorageRegrouper
from exapaths.move_to_ops.preplanned import (
    preplan_pathsampling, add_storage_every_n, edges_to_dag
)

import logging
_logger = logging.getLogger(__name__)
def create_batched_dag(scheme, nsteps, *, max_expensive_tasks=1,
                       store_every=1):
    dag = edges_to_dag(preplan_pathsampling(scheme, nsteps))
    _logger.info(f"Move DAG: {dag}")
    move_grouper = OPSTaskGrouper(max_expensive=max_expensive_tasks)
    move_batched = move_grouper.rebatch(dag)
    _logger.info(f"Batched move DAG: {move_batched}")
    with_storage = add_storage_every_n(move_batched, store_every)
    _logger.info(f"Storage-added DAG: {with_storage}")
    storage_grouper = StorageRegrouper()
    final = storage_grouper.rebatch(with_storage)
    _logger.info(f"Final DAG: {final}")
    return final
