FROM astrocrpublic.azurecr.io/runtime:3.0-7

# change user
USER root

# Copy the whole telethon_sessions directory
COPY telethon_sessions/ /usr/local/airflow/telethon_sessions/
RUN chown -R 50000:50000 /usr/local/airflow/telethon_sessions