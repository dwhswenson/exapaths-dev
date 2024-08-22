import string
import pathlib
try:
    import boto3
except ImportError:
    HAS_AWS = False
else:
    HAS_AWS = True


# need to make these storable, but that should be easy
class BatchOrchestrator:
    def __init__(self, template, jobs_dir='.', working_dir='.'):
        with open(template, mode='r') as f:
            self.template = string.Template(f.read())

        self.jobs_dir = pathlib.Path(jobs_dir)
        self.working_dir = pathlib.Path(working_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.working_dir.mkdir(parents=True, exist_ok=True)

    def submit_job(self, taskid, dependencies) -> str:
        """Return the jobid"""
        raise NotImplementedError()

    def submit_graph(self, taskgraph):
        task_to_jobid = {}
        for taskobj in taskgraph.execution_order():
            taskid = taskobj.uuid.hex
            dependencies = [
                task_to_jobid[dep.uuid.hex] for dep, target in taskgraph.edges
                if target == taskobj
            ]
            task_to_jobid[taskid] = self.submit_job(taskid, dependencies)

    def make_executable(self, taskid):
        raise NotImplementedError()

    def fill_template(self, taskid):
        jobfile = str(self.jobs_dir / f"{taskid}.job")
        executable = self.make_executable(taskid)
        with open(jobfile, mode='w') as f:
            f.write(self.template.substitute(EXECUTABLE=executable))
        return jobfile

class AWSBatchOrchestrator(BatchOrchestrator):
    def __init__(self, template, queue):
        if not HAS_AWS:
            raise RuntimeError("AWS integration requires boto3 installed")

        self.queue = queue

        super().__init__(template)

    def submit_job(self, taskid, dependencies):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/submit_job.html
        resp = boto3.submit_job(
            jobName=taskid,
            jobQueue=self.queue,
            dependsOn=[
                {'jobId': dep, 'type': 'SEQUENTIAL'}
                for dep in dependencies
            ],
            jobDefinition=..., # TODO: what is the job definition here?
        )
        return resp['jobId']

