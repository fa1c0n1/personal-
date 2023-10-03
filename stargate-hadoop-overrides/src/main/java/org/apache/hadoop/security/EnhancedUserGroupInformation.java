package org.apache.hadoop.security;

import lombok.SneakyThrows;
import org.slf4j.Logger;

import javax.security.auth.Subject;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static org.apache.hadoop.security.IdMappingConstant.UNKNOWN_USER;

public final class EnhancedUserGroupInformation extends UserGroupInformation {
    private static final Logger LOGGER = logger(MethodHandles.lookup().lookupClass());

    public EnhancedUserGroupInformation(final Subject subject) {
        super(subject);
    }

    public static EnhancedUserGroupInformation enhancedProxyUser(final UserGroupInformation originalUser) {
        EnhancedUserGroupInformation newUser = new EnhancedUserGroupInformation(originalUser.getSubject());
        newUser.setAuthenticationMethod(originalUser.getAuthenticationMethod());
        return newUser;
    }

    public static EnhancedUserGroupInformation createRemoteUser(final String user, final SaslRpcServer.AuthMethod authMethod) {
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException("Null user");
        }
        Subject subject = new Subject();
        subject.getPrincipals().add(new User(user));
        EnhancedUserGroupInformation result = new EnhancedUserGroupInformation(subject);
        result.setAuthenticationMethod(authMethod);
        return result;
    }

    public static UserGroupInformation getCurrentUser() throws IOException {
        AccessControlContext context = AccessController.getContext();
        Subject subject = Subject.getSubject(context);
        if (subject == null || subject.getPrincipals(User.class).isEmpty()) {
            return getLoginUser();
        } else {
            UserGroupInformation ugi = new UserGroupInformation(subject);
            Subject modifiedSubject = getModifiedSubject(ugi, subject);
            if (modifiedSubject == subject) {
                return ugi;
            } else {
                return new EnhancedUserGroupInformation(modifiedSubject);
            }
        }
    }

    @SneakyThrows
    public static Subject getModifiedSubject(UserGroupInformation ugi, Subject providedSubject) {
        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        Subject subject;
        if (loggedInUser == null || loggedInUser.getSubject() == null) {
            subject = providedSubject;
        } else {
            if (ugi.getRealAuthenticationMethod().getAuthMethod() != loggedInUser.getAuthenticationMethod().getAuthMethod()) {
                LOGGER.debug("subject's authMethod != loggedInUser's authMethod. Using loggedInUser's subject");
                subject = loggedInUser.getSubject();
            } else if (ugi.getRealAuthenticationMethod() == AuthenticationMethod.SIMPLE && UNKNOWN_USER.equalsIgnoreCase(ugi.getUserName())) {
                LOGGER.debug("subject's authMethod is SIMPLE with unknown user. Using loggedInUser's subject");
                subject = loggedInUser.getSubject();
            } else if (loggedInUser.getAuthenticationMethod() == AuthenticationMethod.KERBEROS) {
                LOGGER.debug("Using loggedInUser's subject");
                subject = loggedInUser.getSubject();
            } else {
                subject = providedSubject;
            }
        }
        return subject;
    }

    @Override
    public String toString() {
        return super.toString() + "(enhanced)";
    }

    @SneakyThrows
    @Override
    public <T> T doAs(final PrivilegedAction<T> action) {
        Subject subject = enhancedSubject();
        if (LOG.isDebugEnabled()) {
            String where = new Throwable().getStackTrace()[2].toString();
            LOG.debug("PrivilegedAction as:" + this + " from:" + where);
        }
        return Subject.doAs(subject, action);
    }

    private Subject enhancedSubject() throws IOException {
        return getModifiedSubject(this, this.getSubject());
    }

    @Override
    public <T> T doAs(final PrivilegedExceptionAction<T> action) throws IOException, InterruptedException {
        Subject subject = enhancedSubject();
        try {
            if (LOG.isDebugEnabled()) {
                String where = new Throwable().getStackTrace()[2].toString();
                LOG.debug("PrivilegedExceptionAction as:" + this + " from:" + where);
            }
            LOGGER.debug("Performing EnhancedUserGroupInformation.doAs");
            return Subject.doAs(subject, action);
        } catch (PrivilegedActionException pae) {
            Throwable cause = pae.getCause();
            if (LOG.isDebugEnabled()) {
                LOG.debug("PrivilegedActionException as:" + this + " cause:" + cause);
            }
            if (cause == null) {
                throw new RuntimeException("PrivilegedActionException with no underlying cause. UGI [" + this + "]" + ": " + pae, pae);
            } else if (cause instanceof IOException) {
                throw (IOException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else {
                throw new UndeclaredThrowableException(cause);
            }
        }
    }
}
