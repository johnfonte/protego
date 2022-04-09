package protego;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ParallelHashing
{
	static final String RESOURCES_RELATIVE_PATH = "src/main/resources/";
	static final String HEXES = "0123456789abcdef";

	public static ThreadPoolExecutor getExecutorService(int poolSize, int maxPoolSize) {
		long threadPoolShutDownTime = 1L;
		ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, maxPoolSize, threadPoolShutDownTime, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		executorService.allowCoreThreadTimeOut(true);
		return executorService;
	}

	static byte[] makeServerCall(String urlString) throws IOException, NoSuchAlgorithmException
	{
		URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");

		if (conn.getResponseCode() != 200)
		{
			throw new RuntimeException("Non 200 Response : " + conn.getResponseCode());
		}
		MessageDigest md = MessageDigest.getInstance("MD5");

		try (InputStream is = conn.getInputStream();
			DigestInputStream dis = new DigestInputStream(is, md))
		{
			while (dis.read() != -1);
		}
		return md.digest();
	}


	public static String getMD5Checksum(byte[] bytes)
	{
		//System.out.println(Thread.currentThread().getId());
		if (bytes == null)
		{
			return null;
		}
		StringBuilder sb = new StringBuilder(2 * bytes.length);

		for (final byte b : bytes)
		{
			sb.append(HEXES.charAt((b & 0xF0) >> 4)).append(HEXES.charAt((b & 0x0F)));
		}
		return sb.toString();
	}

	public static List<String> getFileValues(String filename) {
		List<String> values = new ArrayList<>();
		File file = new File(RESOURCES_RELATIVE_PATH + filename);
		try (FileReader fr = new FileReader(file.getAbsolutePath());
			BufferedReader in = new BufferedReader(fr)) {

			String line;
			while ((line = in.readLine()) != null)
			{
				if (line.isBlank()) {
					continue;
				}
				values.add(line.trim());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return values;
	}

	public static void main(String[] args)
	{
		int poolSize = 5;
		if (args.length > 0) {
			try {
				poolSize = Integer.parseInt(args[0]);
			} catch (NumberFormatException nfe) {
				System.out.println("Parallel thread value provided is not numeric");
			}
		}
		List<String> urls = getFileValues("urls.txt");

		List<Callable<String>> callables = new ArrayList<>(urls.size());
		urls.forEach(urlString -> callables.add(() -> getMD5Checksum(makeServerCall(urlString))));

		try
		{
			for (Future<String> hashFuture : getExecutorService(poolSize,poolSize).invokeAll(callables)) {
				System.out.println(hashFuture.get());
			}
		}
		catch (InterruptedException | ExecutionException e)
		{
			e.printStackTrace();
		}
	}
}
