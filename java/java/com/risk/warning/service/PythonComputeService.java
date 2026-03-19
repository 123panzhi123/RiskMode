package com.risk.warning.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.risk.warning.dto.RiskEvaluationResultDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/**
 * 处理 Java 与 Python 跨语言交互的核心服务类
 * 采用 ProcessBuilder 唤起底层操作系统进程进行通信
 */
@Service
public class PythonComputeService {

    private static final Logger logger = LoggerFactory.getLogger(PythonComputeService.class);
    private final ObjectMapper objectMapper;

    // 构造器注入 Jackson 的 ObjectMapper，用于 JSON 反序列化
    public PythonComputeService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 调用 Python 算法脚本执行风险评估
     * * @param tsCode 股票代码
     * @return 解析后的风险评估结果 DTO
     */
    public RiskEvaluationResultDTO executePythonRiskEvaluation(String tsCode) {
        logger.info("开始跨语言调用 Python 引擎，评估股票代码: {}", tsCode);
        Process process = null;

        try {
            // 避坑 1: 严格拆分命令为 List，绝对不拼接成长字符串。
            // 强制使用 -u 参数关闭 Python 输出缓冲，确保日志能被 Java 端实时捕捉！
            // 注意：部署时需要将 "/path/to/script.py" 替换为实际路径，或写入 application.yml 中
//            ProcessBuilder processBuilder = new ProcessBuilder(
//                    Arrays.asList("python", "-u", "/path/to/script.py", "--ts_code", tsCode)
//            );

//t1
//D:\CodeFile\Duoweidesign\CodeduoweiCode\algorithm_engine\evaluate_risk.py
            // 使用虚拟环境的 Python 解释器（确保能找到 sqlalchemy、pymysql 等依赖）
            ProcessBuilder processBuilder = new ProcessBuilder(
                    Arrays.asList("D:\\developpython\\python_exe_file\\Scripts\\python.exe", "-u", "D:\\CodeFile\\Duoweidesign\\CodeduoweiCode\\algorithm_engine\\evaluate_risk.py", "--ts_code", tsCode)
            );


            // 关键修改：不再合并 stderr 到 stdout！
            // Python 端已将日志输出到 stderr，stdout 只输出纯净 JSON
            // 我们需要同时消费 stdout 和 stderr，防止缓冲区满导致死锁
            processBuilder.redirectErrorStream(false);

            // 启动子进程
            process = processBuilder.start();

            // 使用独立线程消费 stderr（Python 日志），防止缓冲区阻塞
            final Process finalProcess = process;
            Thread stderrThread = new Thread(() -> {
                try (BufferedReader errReader = new BufferedReader(new InputStreamReader(finalProcess.getErrorStream()))) {
                    String errLine;
                    while ((errLine = errReader.readLine()) != null) {
                        // 使用 INFO 级别打印 Python 日志，方便排查问题
                        logger.info("Python 日志: {}", errLine);
                    }
                } catch (IOException e) {
                    logger.warn("读取 Python stderr 时出错: {}", e.getMessage());
                }
            });
            stderrThread.setDaemon(true);
            stderrThread.start();

            // 主线程消费 stdout（纯净 JSON）
            StringBuilder outputBuilder = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    outputBuilder.append(line);
                }
            }

            // 等待子进程结束
            int exitCode = process.waitFor();
            stderrThread.join(5000); // 等待 stderr 线程结束，最多 5 秒

            if (exitCode != 0) {
                logger.error("Python 脚本异常退出，退出码: {}。stdout 输出: {}", exitCode, outputBuilder.toString());
                return null;
            }

            // stdout 中应该只有一行纯净的 JSON
            String jsonResult = outputBuilder.toString().trim();
            logger.info("Python 引擎计算完毕，JSON 返回值: {}", jsonResult);

            // 使用 Jackson 反序列化为 Java 对象
            return objectMapper.readValue(jsonResult, RiskEvaluationResultDTO.class);

        } catch (IOException e) {
            // 捕获进程启动、流读取以及 Jackson 序列化失败相关的异常
            logger.error("I/O 异常导致跨语言调用失败 (股票代码: {}): ", tsCode, e);
        } catch (InterruptedException e) {
            // 捕获进程等待被意外打断的异常
            logger.error("Java 主线程等待 Python 进程时被意外中断: ", e);
            // 遵循 Java 并发规范：恢复当前线程的中断状态
            Thread.currentThread().interrupt();
        } finally {
            // 终极兜底：确保无论发生什么异常，底层的僵尸进程都会被强行销毁
            if (process != null && process.isAlive()) {
                process.destroy();
                logger.warn("强制销毁未正常结束的 Python 子进程。");
            }
        }

        return null; // 执行失败时返回 null
    }
}