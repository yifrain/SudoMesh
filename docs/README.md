# SwarmSolve — Documentation

Detailed, code-level design docs in three languages. Pick yours:

| Language | Detailed Architecture | Oral-exam Demo Script | Optimization Design |
|----------|-----------------------|-----------------------|---------------------|
| English | [architecture.en.md](architecture.en.md) | [demo-script.md](demo-script.md) | [optimizations.en.md](optimizations.en.md) |
| 简体中文 | [architecture.zh-CN.md](architecture.zh-CN.md) | [demo-script.zh-CN.md](demo-script.zh-CN.md) | [optimizations.zh-CN.md](optimizations.zh-CN.md) |
| 繁體中文 | [architecture.zh-TW.md](architecture.zh-TW.md) | — | [optimizations.zh-TW.md](optimizations.zh-TW.md) |

> **Optimization Design** covers three enhancements over the current baseline:
> random-ID probing & cold-start, unsolvable detection (`DONE_SPLIT`/`DONE_EXHAUSTED`
> bottom-up aggregation), and a replicated root task to remove the single point.
> **优化设计** 涵盖三项增强：Random ID 探测与冷启动、无解判定
> （`DONE_SPLIT`/`DONE_EXHAUSTED` 自底向上聚合）、以及根任务多副本消除单点。

For a high-level overview, quickstart and the team split, see the
project [`README.md`](../README.md) at the repo root. The **demo script** is a
step-by-step talking guide for the live oral-exam demo (commands, expected
output, highlights, 5-member speaking split, and anticipated Q&A).

各文档内容一致（仅语言不同），包含：分层架构、逐模块代码走读、消息协议、
端到端流程、三个演示（容错 / 实时仪表盘 / 穷举加速基准）、设计取舍与扩展点。

## Project Report (for submission)

[`report.md`](report.md) — English project report **draft** following the course's
required chapter structure (Abstract → Introduction → State of the Art → Design →
Implementation → Evaluation → Discussion → Conclusion → References → Appendix). It
embeds the measured Evaluation numbers from `swarmsolve evaluate` and Mermaid figures.
Before submission: fill in team names/IDs, export figures to images, re-run the
evaluation with `--repeats 5`, and convert to a paginated PDF (code in the Appendix,
not inline in the body).
