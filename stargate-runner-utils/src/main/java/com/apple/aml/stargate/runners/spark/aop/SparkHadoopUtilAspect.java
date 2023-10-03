package com.apple.aml.stargate.runners.spark.aop;

import org.apache.hadoop.security.EnhancedUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class SparkHadoopUtilAspect {
    @Pointcut("execution (* org.apache.spark.deploy.SparkHadoopUtil.createSparkUser())")
    public void createSparkUser() {
    }

    @Around("(createSparkUser())")
    public Object proxySparkUser(final ProceedingJoinPoint pjp) throws Throwable {
        return createSGProxySparkUser(pjp);
    }

    public static Object createSGProxySparkUser(final ProceedingJoinPoint pjp) throws Throwable {
        UserGroupInformation originalUser = (UserGroupInformation) pjp.proceed(new Object[]{});
        if (originalUser instanceof EnhancedUserGroupInformation) {
            return originalUser;
        }
        EnhancedUserGroupInformation newUser = EnhancedUserGroupInformation.enhancedProxyUser(originalUser);
        newUser.addCredentials(originalUser.getCredentials());
        return newUser;
    }
}
