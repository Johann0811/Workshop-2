import inspect
import airflow.utils.db_manager as m

path = inspect.getfile(m)
with open(path, "r", encoding="utf-8") as f:
    content = f.read()

old1 = "        db_exists = self.get_current_revision()\n        if db_exists:"
new1 = "        db_exists = self.get_current_revision()\n        self.session.commit()\n        if db_exists:"

old2 = "        config = self.get_alembic_config()\n        command.stamp(config, \"head\")"
new2 = "        self.session.commit()\n        config = self.get_alembic_config()\n        command.stamp(config, \"head\")"

if old1 not in content or old2 not in content:
    print("Warning or already patched in", path)
else:
    content = content.replace(old1, new1).replace(old2, new2)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Successfully patched Airflow db_manager.py for MySQL metadata locks.")
