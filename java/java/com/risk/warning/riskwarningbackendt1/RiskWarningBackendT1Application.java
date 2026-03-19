package com.risk.warning.riskwarningbackendt1;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


// 注意这里：直接在原有的注解里加上 scanBasePackages
@SpringBootApplication(scanBasePackages = {"com.risk.warning"})
@MapperScan("com.risk.warning.mapper")
public class RiskWarningBackendT1Application {
    public static void main(String[] args) {
        SpringApplication.run(RiskWarningBackendT1Application.class, args);
    }
}