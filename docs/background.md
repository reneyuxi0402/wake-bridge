# 项目背景与目标

Wake Bridge 最初来自一个公共产品问题：能否让 agent 不必持续轮询通知，也能在模型回合之外发生事件时重新获得注意力。

常见的 agent harness 可以运行一次模型回合，却不一定拥有独立、耐久的注意力调度层。邮件、论坛、群聊或未来待办发生变化时，
使用者往往只能让 agent 反复轮询，依赖某个 runner 自带的定时器，或再次由人开启对话。只要进程重启、任务结束或前端更换，
这些临时安排就可能失去连续性。

Wake Bridge 把这部分拆成一个独立的本地服务：

- Source Connector 只报告“发生了什么”；
- agent-owned policy 决定这件事是否以及何时值得重新获得注意力；
- durable claim、batch 与 outbox 保存尚未完成的安排；
- Host Adapter 使用自己真正拥有的 session lifecycle 完成精确投递；
- agent 醒来后再读取权威来源，并确认看见、处理、稍后再看或忽略。

这使 agent 可以为未来的自己建立可靠安排：什么时候醒来、为什么醒来、醒来后去哪里重新取得事实。安排不依赖人再次发起
对话，也不要求某一个模型回合持续运行。用户正在交谈时，可信 Host Adapter 还可以用短期 presence lease 让普通后台事项延后。

Wake Bridge 不负责 agent 的记忆、身份恢复、上下文压缩或长期 loop。它也不猜测最近窗口，不直接承诺启动某个 CLI、Desktop
或 runtime。不同宿主拥有不同的 session 生命周期，因此具体唤醒动作必须由该宿主或集成方实现 Host Adapter。

项目的目标是提供稳定、可审计、可扩展的注意力基础设施，而不是成为另一个 agent runtime 或聊天前端。

