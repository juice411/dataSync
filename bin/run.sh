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
for file in ${lib_dir}/*.jar; do
  classpath="${classpath}:${file}"
done

# 移除 classpath 开头的冒号
classpath="${classpath:1}"

# 构建完整的 Java 命令
java_cmd="${java_home}/bin/java ${jvm_options} -cp ${classpath} com.dtxy.sync.dm2orcl.DirectoryMonitor"

# 执行 Java 命令
${java_cmd}

#最终执行nohup bin/run.sh >/dev/null 2>&1 &来后台运行该服务

