package com.apple.aml.stargate.common.utils;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;

public class DynamicURLClassLoader extends URLClassLoader {
    public DynamicURLClassLoader(final URL[] urls, final ClassLoader parent) {
        super(urls, parent);
    }

    public DynamicURLClassLoader(final URL[] urls) {
        super(urls);
    }

    public DynamicURLClassLoader(final URL[] urls, final ClassLoader parent, final URLStreamHandlerFactory factory) {
        super(urls, parent, factory);
    }

    public DynamicURLClassLoader(final String name, final URL[] urls, final ClassLoader parent) {
        super(name, urls, parent);
    }

    public DynamicURLClassLoader(final String name, final URL[] urls, final ClassLoader parent, final URLStreamHandlerFactory factory) {
        super(name, urls, parent, factory);
    }

    public void addURL(URL url) {
        super.addURL(url);
    }
}
