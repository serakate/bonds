from alembic import context
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig
from pathlib import Path
import sys

# Get the directory of the env.py file
env_dir = Path(__file__).resolve().parent

# Append the relative path to the project directory
sys.path.append(str(env_dir.parent.parent / 'your' / 'project'))  # Replace 'your' and 'project' with the actual directory names

from pool import db  # Replace 'pool' with the actual module name

# Define the function for creating a database connection
def run_migrations_offline():
    # Load the configuration from the alembic.ini file
    config = context.config
    fileConfig(config.config_file_name)

    # Create the database connection
    db_url = "sqlite:///" + str(env_dir / 'pool.db')  # Replace 'pool.db' with the actual database file name
    engine = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # Create and return the database session
    connection = engine.connect()
    context.configure(
        connection=connection,
        target_metadata=db.metadata,
    )

    try:
        with context.begin_transaction():
            context.run_migrations()
    finally:
        connection.close()

# Define the function for creating a database connection during migration
def run_migrations_online():
    # Load the configuration from the alembic.ini file
    config = context.config
    fileConfig(config.config_file_name)

    # Create the database connection
    db_url = "sqlite:///" + str(env_dir / 'pool.db')  # Replace 'pool.db' with the actual database file name
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # Create and return the database session
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=db.metadata,
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()