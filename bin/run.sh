#!/bin/bash

# 指定 Java Home 路径
java_home="/usr/lib/jvm/java-13"

# 设置 JVM 参数
jvm_options="-Xmx2G -Xms1G -XX:+UseG1GC"

# 获取当前脚本所在目录的上一级目录
base_dir="$(dirname "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)")"

# 拼接 lib 目录的路径
lib_dir="${base_dir}/lib"

# 检查 lib 目录是否存在
if [ ! -d "${lib_dir}" ]; then
  echo "lib 目录不存在: ${lib_dir}"
  exit 1
fi

# 构建 classpath
classpath=""
for file in "${lib_dir}"/*.jar; do
  classpath="${classpath}:${file}"
done

# 移除 classpath 开头的冒号
classpath="${classpath:1}"

# 构建完整的 Java 命令
java_cmd="${java_home}/bin/java ${jvm_options} -cp ${classpath} com.dtxy.sync.dm2orcl.DirectoryMonitor"

# 定义应用程序名称
app_name="DirectoryMonitor"

# 定义日志文件路径
log_file="${base_dir}/logs/application.log"

start() {
  if is_running; then
    echo "${app_name} 已经在运行中"
  else
    echo "正在启动 ${app_name} ..."
    nohup ${java_cmd} >> "${log_file}" 2>&1 &
    echo "${app_name} 启动成功"
  fi
}

stop() {
  if is_running; then
    echo "正在停止 ${app_name} ..."
    pkill -f "${app_name}"
    echo "${app_name} 停止成功"
  else
    echo "${app_name} 未在运行中"
  fi
}

is_running() {
  pgrep -f "${app_name}" >/dev/null 2>&1
}

restart() {
  stop
  start
}

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    restart
    ;;
  *)
    echo "使用方式: $0 {start|stop|restart}"
    exit 1
    ;;
esac
