# cloudcanal-data-process

#### 介绍

本工程汇集了 CloudCanal 数据处理插件，以达成数据自定义 transformation 目标

#### 插件说明

- **wide-table** : 打宽表数据处理插件，主要包括事实表和单维表组合处理
- **data-compare** : 数据对比插件，根据源端数据变化进行业务对账
- **business-alert** : 业务告警插件，根据数据变化趋势做出相应告警

#### 使用说明

- 将需要使用的 CloudCanalProcessor 实现类(如:WideTableProcessorV2_simple)进行适配性改造
- 子工程下 src/main/resources/META-INF/cloudcanal/plugin.properties 中修改为需要使用的类
- 子工程下 `mvn -Dtest -DfailIfNoTests=false -Dmaven.javadoc.skip=true -Dmaven.compile.fork=true clean package` 打包
- CloudCanal 控制台创建任务，并上传子工程 target 下 jar 包(如:wide-table-1.0.0-SNAPSHOT.jar)

