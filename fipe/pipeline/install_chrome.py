# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt update && sudo apt upgrade -y --fix-missing > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC wget  -P /tmp/ https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt install /tmp/google-chrome-stable_current_amd64.deb --fix-broken --fix-missing --yes


# COMMAND ----------

# MAGIC %sh
# MAGIC ls /usr/bin | grep goo


# COMMAND ----------

# MAGIC %sh
# MAGIC export MSG="Installed and found"
# MAGIC echo $MSG
