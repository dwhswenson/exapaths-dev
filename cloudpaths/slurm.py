import subprocess

import click
import pathlib
from cloudpaths.batchorchestrators import BatchOrchestrator
from cloudpaths.create_task_graph import create_task_graph
from cloudpaths.run_task import SimStoreZipStorage
from cloudpaths.move_to_ops.storage_handlers import LocalFileStorageHandler

from paths_cli.parameters import (
    INPUT_FILE, SCHEME, INIT_CONDS, OUTPUT_FILE, N_STEPS_MC,
)

import logging
_logger = logging.getLogger(__name__)

class SLURMOrchestrator(BatchOrchestrator):
    def make_executable(self, taskid):
        return (
            f"python -m cloudpaths.slurm run_task {taskid} --working-dir "
            f"{self.working_dir}"
        )

    def submit_job(self, taskid, dependencies):
        # taskid is a task identified; dependencies are a list of jobids
        jobfile = self.fill_template(taskid)
        dependency_str = ":".join(str(dep) for dep in dependencies)
        cmd = ["sbatch", f"--dependency=afterok:{dependency_str}", jobfile]
        jobid = subprocess.check_output(cmd)
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
@click.option("--template", required=True)
def submit(input_file, output_file, scheme, init_conds, nsteps, template):
    storage = INPUT_FILE.get(input_file)
    scheme = SCHEME.get(storage, scheme)
    init_conds = INIT_CONDS.get(storage, init_conds)

    base_dir = pathlib.Path(output_file).parent
    storage_handler = LocalFileStorageHandler(base_dir / "working")
    objectdb = SimStoreZipStorage(storage_handler)
    task_graph = create_task_graph(scheme, nsteps, objectdb)
    orchestrator = SLURMOrchestrator(template, jobs_dir=base_dir / "jobs")
    orchestrator.submit_graph(task_graph)


@slurm.command()
@click.argument('taskid', type=str)
@click.option('--working-dir')  # TODO: typing/validation on inputs
def run_task(taskid, working_dir):
    storage_handler = LocalFileStorageHandler(working_dir)
    objectdb = SimStoreZipStorage(storage_handler)
    # TODO: move the code that does this somewhere else, probably
    task = MoverTask(taskid)
    task.run_task(objectdb)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    slurm()
