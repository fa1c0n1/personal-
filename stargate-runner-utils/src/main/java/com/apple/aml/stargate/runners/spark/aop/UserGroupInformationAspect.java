package com.apple.aml.stargate.runners.spark.aop;

import org.apache.hadoop.security.EnhancedUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

@Aspect
public class UserGroupInformationAspect {
    @Pointcut("execution (* org.apache.hadoop.security.UserGroupInformation.createRemoteUser(java.lang.String))")
    public void createRemoteUser() {
    }

    @Around("(createRemoteUser()) && args(userName)")
    public Object proxyRemoteUser(final ProceedingJoinPoint pjp, final String userName) throws Throwable {
        UserGroupInformation originalUser = (UserGroupInformation) pjp.proceed(new Object[]{userName});
        if (originalUser instanceof EnhancedUserGroupInformation) {
            return originalUser;
        }
        EnhancedUserGroupInformation newUser = EnhancedUserGroupInformation.createRemoteUser(userName, originalUser.getAuthenticationMethod().getAuthMethod());
        newUser.addCredentials(originalUser.getCredentials());
        return newUser;
    }
}
