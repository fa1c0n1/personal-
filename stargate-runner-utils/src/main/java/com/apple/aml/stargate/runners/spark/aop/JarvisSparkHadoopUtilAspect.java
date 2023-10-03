package com.apple.aml.stargate.runners.spark.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

import static com.apple.aml.stargate.runners.spark.aop.SparkHadoopUtilAspect.createSGProxySparkUser;

@Aspect
public class JarvisSparkHadoopUtilAspect {

    @Pointcut("execution (* org.apache.spark.integration.JarvisSparkHadoopUtil.createSparkUser())")
    public void createSparkUser() {
    }

    @Around("(createSparkUser())")
    public Object proxyPieSparkUser(final ProceedingJoinPoint pjp) throws Throwable {
        return createSGProxySparkUser(pjp);
    }
}
