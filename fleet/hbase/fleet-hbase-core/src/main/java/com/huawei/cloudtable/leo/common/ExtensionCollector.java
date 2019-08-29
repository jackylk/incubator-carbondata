package com.huawei.cloudtable.leo.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public final class ExtensionCollector {

	public static final String REGISTER_FILE_PATH = "META-INF/services/";

	public static <TSuperClass> Iterator<Class<? extends TSuperClass>> getExtensionClasses(
			final Class<TSuperClass> superClass) throws Error {
		return new LazyIterator<>(superClass, Thread.currentThread().getContextClassLoader());
	}

	public static <TSuperClass> Iterator<Class<? extends TSuperClass>> getExtensionClasses(
			final Class<TSuperClass> superClass, final ClassLoader loader)
			throws Error {
		return new LazyIterator<>(superClass, loader);
	}

	private static Iterator<String> parse(final Class<?> superClass,
			final URL URL, final TreeSet<String> classNameSet)
			throws Error {
		final ArrayList<String> classNameList = new ArrayList<>();
		try {
			try (InputStream inputStream = URL.openStream()) {
				try (BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))) {
					int lineNumber = 1;
					while ((lineNumber = parseLine(superClass, URL, buffer, lineNumber, classNameList, classNameSet)) >= 0) {
						// to do nothing.
					}
				}
			}
		} catch (IOException exception) {
			throw new RuntimeException(superClass.getName() + ": " + exception);
		}
		return classNameList.iterator();
	}

	private static int parseLine(final Class<?> superClass,
			final URL URL, final BufferedReader buffer, final int lineNumber,
			final ArrayList<String> classNameList,
			final TreeSet<String> classNameSet) throws Error {
		String line;
		try {
			line = buffer.readLine();
		} catch (IOException exception) {
			throw new RuntimeException(superClass.getName() + ": " + exception.getMessage());
		}
		if (line == null) {
			return -1;
		}
		int index = line.indexOf('#');
		if (index >= 0) {
			line = line.substring(0, index);
		}
		line = line.trim();
		final int lineLength = line.length();
		if (lineLength != 0) {
			if ((line.indexOf(' ') >= 0) || (line.indexOf('\t') >= 0)) {
				throw new RuntimeException(superClass.getName() + ": " + URL.toString() + ": " + lineNumber + ": Illegal configuration-file syntax.");
			}
			int codePoint = line.codePointAt(0);
			if (!Character.isJavaIdentifierStart(codePoint)) {
				throw new RuntimeException(superClass.getName() + ": " + URL.toString() + ": " + lineNumber + ": Illegal extension-class name: " + line);
			}
			for (int i = Character.charCount(codePoint); i < lineLength; i += Character.charCount(codePoint)) {
				codePoint = line.codePointAt(i);
				if (!Character.isJavaIdentifierPart(codePoint) && (codePoint != '.')) {
					throw new RuntimeException(superClass.getName() + ": " + URL.toString() + ": " + lineNumber + ": Illegal extension-class name: " + line);
				}
			}
			if (!classNameSet.contains(line)) {
				classNameList.add(line);
				classNameSet.add(line);
			}
		}
		return lineNumber + 1;
	}

	private ExtensionCollector() {
		// to do nothing.
	}

	private static final class LazyIterator<TSuperClass> implements
			Iterator<Class<? extends TSuperClass>> {

		LazyIterator(final Class<TSuperClass> superClass,
				final ClassLoader classLoader) {
			this.superClass = superClass;
			this.classLoader = classLoader;
		}

		private final Class<TSuperClass> superClass;

		private final ClassLoader classLoader;

		private final TreeSet<String> classNameSet = new TreeSet<>();

		private Iterator<String> classNameIterator = null;

		private String nextClassName = null;

		private Enumeration<URL> configurationFiles = null;

		public boolean hasNext() throws Error {
			if (this.nextClassName != null) {
				return true;
			}
			if (this.configurationFiles == null) {
				try {
					final String fullName = REGISTER_FILE_PATH + this.superClass.getName();
					if (this.classLoader == null) {
						this.configurationFiles = ClassLoader.getSystemResources(fullName);
					} else {
						this.configurationFiles = this.classLoader.getResources(fullName);
					}
				} catch (IOException exception) {
					throw new RuntimeException(this.superClass.getName() + ": " + exception.getMessage());
				}
			}
			while ((this.classNameIterator == null) || !this.classNameIterator.hasNext()) {
				if (!this.configurationFiles.hasMoreElements()) {
					return false;
				}
				this.classNameIterator = parse(this.superClass, this.configurationFiles.nextElement(), this.classNameSet);
			}
			this.nextClassName = this.classNameIterator.next();
			return true;
		}

		@SuppressWarnings("unchecked")
		public Class<? extends TSuperClass> next() throws Error {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			final String className = this.nextClassName;
			this.nextClassName = null;
			final Class<?> clazz;
			try {
				clazz = Class.forName(className, true, this.classLoader);
			} catch (ClassNotFoundException exception) {
				throw new RuntimeException(this.superClass.getName() + ": " + "Extension " + className + " not found.");
			}
			if (this.superClass.isAssignableFrom(clazz)) {
				return (Class<? extends TSuperClass>) clazz;
			} else {
				throw new RuntimeException(this.superClass.getName() + ": " + "Extension " + className + " is not a subclass for superclass.");
			}
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}
