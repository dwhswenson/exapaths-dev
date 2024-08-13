import sqlalchemy as sqla
import exorcist

class TaskStatusDB(exorcist.TaskStatusDB):
    @staticmethod
    def _create_empty_db(metadata, engine):
        task_type_table = sqla.Table(
            "task_types",
            metadata,
            sqla.Column("taskid", sqla.String,
                        sqla.ForeignKey("tasks.taskid")),
            sqla.Column("task_type", sqla.String)
        )
        return exorcist.TaskStatusDB._create_empty_db(metadata, engine)

    @property
    def task_types_table(self):
        return self.metadata.tables['task_types']

    def add_task_type(self, taskid, task_type):
        with self.engine.begin() as conn:
            res = conn.execute(
                sqla.insert(self.task_types_table).values([
                    {'taskid': taskid, 'task_type': task_type},
                ])
            )

    def add_task_network(self, taskid_network, max_tries):
        super().add_task_network(taskid_network, max_tries)
        for node in taskid_network.nodes:
            self.add_task_type(node, taskid_network.nodes[node]['obj'].TYPE)

    def get_task_type(self, taskid):

        with self.engine.connect() as conn:
            res = list(conn.execute(
                sqla.select(self.task_types_table)
                .where(self.task_types_table.c.taskid == taskid)
            ))
        assert len(res) == 1
        return res[0].task_type


