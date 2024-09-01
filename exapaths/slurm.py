import subprocess
import sys

import click
import pathlib
from exapaths.batchorchestrators import BatchOrchestrator
from exapaths.create_task_graph import create_task_graph
from exapaths.run_task import SimStoreZipStorage, TASK_DISPATCH
from exapaths.move_to_ops.storage_handlers import LocalFileStorageHandler
from exapaths.worker import MoverTask
from exapaths.simulation import ExapathsSimulation

from paths_cli.parameters import (
    INPUT_FILE, SCHEME, INIT_CONDS, OUTPUT_FILE, N_STEPS_MC,
)

import logging
_logger = logging.getLogger(__name__)

class SLURMOrchestrator(BatchOrchestrator):
    def make_executable(self, taskid):
        return (
            f"python -m exapaths.slurm run-task {taskid} --working-dir "
            f"{self.working_dir}"
        )

    def _parse_sbatch_output(self, output: str) -> str:
        """Parse the output of an sbatch command to get the jobid.

        This is a separate function because some clusters use modified versions
        of sbatch that return something different (e.g., only the job ID).

        Input is assumed to be string and to have trailing newlines chopped.
        """
        # 'Submitted batch job 5'
        expected_init = "Submitted batch job "
        if not output.startswith(expected_init):
            raise RuntimeError(
                    "Unexpected output from `sbatch`: Expected string "
                    f"starting with '{expected_init}'; received '{output}'."
            )
        # we cast to int to raise an error if this is not a valid jobid
        jobid = str(int(output[len(expected_init):]))
        return jobid


    def submit_job(self, taskid, dependencies):
        # taskid is a task identified; dependencies are a list of jobids
        jobfile = self.fill_template(taskid)
        cmd = ["sbatch"]
        if dependencies:
            dependency_str = "--dependency=afterok:"
            dependency_str += ":".join(str(dep) for dep in dependencies)
            cmd.append(dependency_str)

        cmd.append(jobfile)
        _logger.info(f"Running: {cmd}")
        result = subprocess.check_output(cmd).decode(sys.stdout.encoding).strip()
        jobid = self._parse_sbatch_output(result)
        return jobid


@click.group()
def slurm():
    pass

@slurm.command()
@INPUT_FILE.clicked()
@OUTPUT_FILE.clicked()
@SCHEME.clicked()
@INIT_CONDS.clicked()
@N_STEPS_MC
@click.option("--store-every", default=1)
@click.option("--template", required=True)
def submit(input_file, output_file, scheme, init_conds, nsteps, store_every,
           template):
    import openpathsampling as paths
    _logger.info(f"Loading objects from {input_file}....")
    storage = INPUT_FILE.get(input_file)
    scheme = SCHEME.get(storage, scheme)
    init_conds = INIT_CONDS.get(storage, init_conds)

    base_dir = pathlib.Path(output_file).parent
    working = base_dir / "working"
    storage_handler = LocalFileStorageHandler(working)
    objectdb = SimStoreZipStorage(storage_handler)
    _logger.info(f"Preplanning the path sampling simulation....")
    task_graph = create_task_graph(scheme, nsteps, objectdb,
                                   store_every=store_every)
    orchestrator = SLURMOrchestrator(template, jobs_dir=working / "jobs",
                                     working_dir=working)

    init_conds = scheme.initial_conditions_from_trajectories(init_conds)
    _logger.info("Storing the initial conditions....")
    sim = ExapathsSimulation()
    objectdb.save_initial_conditions(scheme, init_conds, mccycle=0,
                                     simulation=sim)
    _logger.info("Submitting the jobs to SLURM")
    orchestrator.submit_graph(task_graph)


@slurm.command()
@click.argument('taskid', type=str)
@click.option('--working-dir')  # TODO: typing/validation on inputs
def run_task(taskid, working_dir):
    storage_handler = LocalFileStorageHandler(working_dir)
    objectdb = SimStoreZipStorage(storage_handler)
    with objectdb.load_task(taskid) as task:
        runner = TASK_DISPATCH[task.TYPE]
        runner(task, objectdb)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, force=True)
    debug_loggers = [
    ]
    for logger_name in debug_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

    warning_loggers = [
        'openpathsampling.experimental',
    ]
    for logger_name in warning_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)

    slurm()
