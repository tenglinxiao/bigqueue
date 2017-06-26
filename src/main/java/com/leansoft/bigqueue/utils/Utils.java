package com.leansoft.bigqueue.utils;

import java.lang.management.ManagementFactory;

public class Utils {

	public static long getCurrentPid() {
		String processName = ManagementFactory.getRuntimeMXBean().getName();
		if (processName != null) {
			return Long.parseLong(processName.split("@")[0]);
		}
		return 0;
	}
}
