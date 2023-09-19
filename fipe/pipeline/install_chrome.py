# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt update && sudo apt upgrade -y --fix-missing > /dev/null 2>&1 &

# COMMAND ----------

# MAGIC %sh
# MAGIC wget  -P /usr/ https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt install /usr/google-chrome-stable_current_amd64.deb --fix-broken --fix-missing --yes
