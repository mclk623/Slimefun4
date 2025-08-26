package io.github.thebusybiscuit.slimefun4.implementation.tasks;

import com.xzavier0722.mc.plugin.slimefun4.storage.controller.SlimefunBlockData;
import com.xzavier0722.mc.plugin.slimefun4.storage.util.StorageCacheUtils;
import io.github.bakedlibs.dough.blocks.ChunkPosition;
import io.github.thebusybiscuit.slimefun4.api.items.SlimefunItem;
import io.github.thebusybiscuit.slimefun4.core.ticker.TickLocation;
import io.github.thebusybiscuit.slimefun4.implementation.Slimefun;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Level;
import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import lombok.Getter;
import lombok.Setter;
import me.matl114.matlib.Algorithms.DataStructures.Struct.Triplet;
import me.mrCookieSlime.Slimefun.Objects.handlers.BlockTicker;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.block.Block;
import org.bukkit.scheduler.BukkitTask;

public class AsyncTickerTask extends TickerTask {
    public void setUseAsync(boolean useAsync) {
        if(useAsync!=this.useAsync){
            this.useAsync = useAsync;
            if(this.useAsync&& (parallelismLevel <= 0||parallelismLevel > 2)){
                this.parallelismLevel = 1;
            }
        }
    }
    @Getter
    private boolean useAsync = Slimefun.getCfg().getOrSetDefault("URID.enable-async-tickers", false);
    public void setParallelismLevel(int parallelismLevel) {
        this.parallelismLevel = Math.min(2,Math.max(parallelismLevel,0));
    }
    @Getter
    private int parallelismLevel = Slimefun.getCfg().getOrSetDefault("URID.async-tickers-settings.paral-level",1);

    private final int overrideThreadCount = Slimefun.getCfg().getOrSetDefault("URID.async-tickers-settings.override-thread-count",-1);
    @Getter
    @Setter
    private boolean debugMode = false;
    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        resetTheadPool();
    }
    private boolean poolResetting = false;
    @Getter
    private int threadCount =overrideThreadCount>1?overrideThreadCount: (Math.min(32767, Runtime.getRuntime().availableProcessors()) / 2) ;

    public void setPoolType(String poolType){
        this.poolType = poolType;
        resetTheadPool();
    }
    private String poolType = Slimefun.getCfg().getOrSetDefault("URID.async-tickers-settings.pool-type","ForkJoinPool"); // "ThreadPoolExecutor";
    private AbstractExecutorService tickerThreadPool;

    public AsyncTickerTask() {
        Slimefun.logger().log(Level.INFO, "Setting up tick task");
        if (useAsync) {
            Slimefun.logger().log(Level.INFO, "Async Ticker enabled");
            if(parallelismLevel < 0 ||parallelismLevel >2 ){
                for (int i = 0 ; i < 10 ;++i){
                    Slimefun.logger().log(Level.INFO, "你的多线程并行等级不在有效范围(0~2),已禁用多线程优化");
                }
                parallelismLevel = 0;
                useAsync = false;
            }else if(parallelismLevel == 0){
                for (int i = 0 ; i < 10 ;++i){
                    Slimefun.logger().log(Level.INFO, "你的多线程并行等级为0,将不会开启多线程优化");
                }
                useAsync = false;
            }else{
                for (int i = 0 ; i < 10*parallelismLevel ;++i){
                    Slimefun.logger().log(Level.INFO, "{0} 级多线程优化已经启动,请确保你知道你在做什么!",parallelismLevel);
                    Slimefun.logger().log(Level.INFO, "你可以在config.yml 的URID.enable-async-tickers 设置是否启用!");
                    Slimefun.logger().log(Level.INFO, "或者在config.yml 的URID.async-tickers-settings.paral-level 设置并行等级!");
                }
                resetTheadPool();
            }
        } else {
            Slimefun.logger().log(Level.INFO, "Async Ticker disabled");
        }
    }

    public synchronized void resetTheadPool() {
        if (tickerThreadPool != null && tickerThreadPool != ForkJoinPool.commonPool()) {
            tickerThreadPool.shutdown();
        }
        tickerThreadPool = genPool();
        poolResetting = true;
        Bukkit.getScheduler().runTaskLaterAsynchronously(Slimefun.instance(),()->this.poolResetting=false,20*15);
    }

    public AbstractExecutorService genPool() {
        AbstractExecutorService result;
        if ("ThreadPoolExecutor".equals(poolType)) {
            var re = new ThreadPoolExecutor(
                    threadCount,
                    2 * threadCount - 2,
                    60,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(50000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            Slimefun.logger()
                    .log(
                            Level.INFO,
                            "Starting ticker ThreadPoolExecutor with core pool size " + re.getCorePoolSize()
                                    + " and max pool size " + re.getMaximumPoolSize());
            result = re;
        } else if ("ForkJoinPool".equals(poolType)) {
            var re = new ForkJoinPool(threadCount);
            Slimefun.logger().log(Level.INFO, "Starting ticker ForkJoinPool with parallelism " + re.getParallelism());
            result = re;
        } else {
            result = ForkJoinPool.commonPool();
            Slimefun.logger().log(Level.INFO, "Starting ticker using common ForkJoinPool");
        }
        return result;
    }

    public AbstractExecutorService getTickerThreadPool() {
        if (tickerThreadPool == null) {
            resetTheadPool();
        }
        return tickerThreadPool;
    }

    private static final Map<ChunkPosition, AtomicInteger> chunkLock = new ConcurrentHashMap<>();
    private void debug(Supplier<String> debug){
        if(debugMode){
            Slimefun.logger().log(Level.INFO, debug.get());
        }
    }
    @Override
    public void run() {
        long start = System.nanoTime();

        if (!useAsync) {
            super.run();
            return;
        }
        if (paused) {
            return;
        }
        try {
            switch(parallelismLevel){
                case 2: runParallismLevel2(start);break;
                case 1: runParallismLevel1(start);break;
                default: throw new RuntimeException("Illegal parallelismLevel: %d".formatted(parallelismLevel));
            }
        } catch (Exception | LinkageError x) {
            Slimefun.logger()
                    .log(
                            Level.SEVERE,
                            x,
                            () -> "An Exception was caught while ticking the Block Tickers Task for Slimefun v"
                                    + Slimefun.getVersion());
            // reset();
        } finally {
            reset();
        }
        long end = System.nanoTime();
        debug(()->"totalCost "+((end-start)/1_000_000)+" ms");
    }
    private static final Map<BlockTicker, ReentrantLock> asyncBlockTickerLock = new ConcurrentHashMap<>();
    private void runParallismLevel1(long startNanos){
        if(running){
            debug(()->"Still Running last task");
            return;
        }
        running = true;
        Slimefun.getProfiler().start();
        Set<BlockTicker> syncTickers = ConcurrentHashMap.newKeySet();
        Set<BlockTicker> asyncTickers = ConcurrentHashMap.newKeySet();
        if(!halted){
            Set<ChunkPosition> loc;
            synchronized (tickingLocations) {
                loc = new HashSet<>(tickingLocations.keySet());
            }
            Map<ChunkPosition, CompletableFuture<Void>> chunkTickProgress = new HashMap<>();
            for (ChunkPosition chunkPosition : loc) {
                if(chunkPosition.isLoaded()){
                    HashSet<TickLocation> locs;
                    synchronized (tickingLocations) {
                        locs = new HashSet<>(tickingLocations.get(chunkPosition));
                    }
                    final ChunkPosition thisChunkPosition = chunkPosition;
                    chunkTickProgress.put(chunkPosition,CompletableFuture.runAsync(()->{
                        if(thisChunkPosition.isLoaded()){
                            tickAsyncChunkProgressParalevel1(thisChunkPosition,locs,syncTickers,asyncTickers);
                        }
                    }));
                }
            }
            CompletableFuture.allOf(chunkTickProgress.values().toArray(CompletableFuture[]::new))
                .orTimeout(10, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    // 超时或者其他异常的处理逻辑
                    if(!halted){
                        Slimefun.logger().log(Level.SEVERE, ex, () -> {
                            return "Timeout or error occurred in AsyncTickTask Lv.1: ";
                        });
                        Slimefun.logger().log(Level.WARNING, () -> {
                            return "Resetting Thread Pool... ";
                        });
                        this.bugs.clear();
                        if (!this.paused) {
                            resetTheadPool();
                        }
                    }
                    return null;
                })
                .join();

        }
        // Start a new tick cycle for every BlockTicker
        for (BlockTicker ticker : asyncTickers) {
            ticker.startNewTick();
        }
        for (BlockTicker ticker : syncTickers) {
            ticker.startNewTick();
        }
        long stopProfiler = System.nanoTime();
        debug(()->"stopping profiler after "+((stopProfiler-startNanos)/1_000_000)+" ms");
        Slimefun.getProfiler().stop();
    }
    public void tickAsyncChunkProgressParalevel1(ChunkPosition chunkPosition,Set<TickLocation> tickLocations,Set<BlockTicker> syncTickers,Set<BlockTicker> asyncTickers){
        Set<Triplet<SlimefunItem,BlockTicker,SlimefunBlockData>> asyncTickingInformation = new LinkedHashSet<>();
        for(TickLocation tickLocation : tickLocations){
            Location l = tickLocation.getLocation(); // 获取Location
            var blockData = StorageCacheUtils.getBlock(l);
            if (blockData == null || !blockData.isDataLoaded() || blockData.isPendingRemove()) {
                continue;
            }
            SlimefunItem item = SlimefunItem.getById(blockData.getSfId());

            if (item != null && item.getBlockTicker() != null) {
                if (item.isDisabledIn(l.getWorld())) {
                    continue;
                }
                try {
                    BlockTicker ticker = item.getBlockTicker();
                    ticker.update();
                    if (ticker.isSynchronized()) {
                        // sync task run later, so lock is not required
                        syncTickers.add(ticker);
                        Slimefun.getProfiler().scheduleEntries(1);
                        /**
                         * We are inserting a new timestamp because synchronized actions
                         * are always ran with a 50ms delay (1 game tick)
                         */
                        Slimefun.runSync(() -> {
                            Block b = l.getBlock();
                            tickBlock(l, item, blockData, System.nanoTime());
                        });
                    } else {
                        asyncTickers.add(ticker);
                        asyncTickingInformation.add(Triplet.of(item,ticker,blockData));
                    }
                } catch (Throwable x) {
                    reportErrors(l, item, x);
                }
            }
        }
        int loopTimes = 0;
        while(true){
            if(loopTimes > 10){
                for(Triplet<SlimefunItem,BlockTicker,SlimefunBlockData> t : asyncTickingInformation){
                    ReentrantLock lock = asyncBlockTickerLock.computeIfAbsent(t.getB(), k -> new ReentrantLock());
                    tryTickBlockWithLock(t.getC().getLocation(), t.getB(),t.getA(),t.getC(),lock);
                }
                break;
            }else {
                Iterator<Triplet<SlimefunItem,BlockTicker,SlimefunBlockData>> iterator = asyncTickingInformation.iterator();
                while(iterator.hasNext()){
                    var t = iterator.next();
                    ReentrantLock lock = asyncBlockTickerLock.computeIfAbsent(t.getB(), k -> new ReentrantLock());
                    if(!lock.isLocked()||lock.isHeldByCurrentThread()){
                        iterator.remove();
                        tryTickBlockWithLock(t.getC().getLocation(), t.getB(),t.getA(),t.getC(),lock);
                    }
                }
            }
            loopTimes++;
        }

    }
    public void tryTickBlockWithLock(Location l, BlockTicker ticker,SlimefunItem item, SlimefunBlockData blockData,ReentrantLock lock){
        long timestamp = 0L;
        lock.lock();
        try {
            timestamp = Slimefun.getProfiler().newEntry();
            if (blockData.isPendingRemove()) {
                return;
            }
            Block b = l.getBlock();
            ticker.tick(b, item, blockData);
        } catch (Throwable x) {
            reportErrors(l, item, x);
        } finally {
            lock.unlock();
            Slimefun.getProfiler().closeEntry(l, item, timestamp);
        }
    }
    public void tickBlockInAsync(Location l, BlockTicker ticker,SlimefunItem item, SlimefunBlockData blockData) {
        long timestamp = Slimefun.getProfiler().newEntry();
        try {
            if (blockData.isPendingRemove()) {
                return;
            }
            Block b = l.getBlock();
            ticker.tick(b, item, blockData);
        } catch (Throwable x) {
            reportErrors(l, item, x);
        } finally {
            // end chunk weak lock when async task end,even if data pending move, this code will run
            Slimefun.getProfiler().closeEntry(l, item, timestamp);
        }
    }

    private void runParallismLevel2(long startNanos){
        // If this method is actually still running... DON'T
        if (running) {
            debug(()->"Still Running");
            return;
        }
        running = true;
        Slimefun.getProfiler().start();
        // each ticker map to a single task chain ,in case of
        // ConcurrentModificationException
        HashMap<BlockTicker, CompletableFuture<Void>> tickers = new HashMap<>();
        // sync tickers
        HashSet<BlockTicker> syncTickers = new HashSet<>();
        // Run our ticker code
        if (!halted) {
            Set<ChunkPosition> loc;
            synchronized (tickingLocations) {
                loc = new HashSet<>(tickingLocations.keySet());
            }
            Map<ChunkPosition, Iterator<TickLocation>> chunkMachineSequence = new HashMap<>(); // 修改为 Iterator<TickLocation>
            for (ChunkPosition entry : loc) {
                try {
                    if (entry.isLoaded()) {
                        HashSet<TickLocation> locs;
                        synchronized (tickingLocations) {
                            locs = new HashSet<>(tickingLocations.get(entry));
                        }
                        chunkMachineSequence.put(entry, locs.iterator());
                    }
                } catch (ArrayIndexOutOfBoundsException | NumberFormatException x) {
                    Slimefun.logger()
                        .log(
                            Level.SEVERE,
                            x,
                            () -> "An Exception has occurred while trying to resolve Chunk: " + entry);
                }
            }
            tickChunkAsyncParalevel2(chunkMachineSequence, tickers, syncTickers);
            CompletableFuture.allOf(tickers.values().toArray(CompletableFuture[]::new))
                .orTimeout(10, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    if(!halted){
                        Slimefun.logger().log(Level.SEVERE, ex, () -> {
                            return "Timeout or error occurred in AsyncTickTask Lv.2: ";
                        });
                        Slimefun.logger().log(Level.WARNING, () -> {
                            return "Resetting Thread Pool... ";
                        });
                        this.bugs.clear();
                        if (!this.paused) {
                            resetTheadPool();
                        }
                    }
                    return null;
                })
                .join();
        }

        // Start a new tick cycle for every BlockTicker
        for (BlockTicker ticker : tickers.keySet()) {
            ticker.startNewTick();
        }
        for (BlockTicker ticker : syncTickers) {
            ticker.startNewTick();
        }
        long stopProfiler = System.nanoTime();
        debug(()->"stopping profiler after "+((stopProfiler-startNanos)/1_000_000)+" ms");
        Slimefun.getProfiler().stop();
    }

    private void tickChunkAsyncParalevel2(
            Map<ChunkPosition, Iterator<TickLocation>> machineSequence, // 修改为 Iterator<TickLocation>
            HashMap<BlockTicker, CompletableFuture<Void>> tickers,
            HashSet<BlockTicker> syncTickers) {
        while (!machineSequence.isEmpty()) {
            var iter = machineSequence.entrySet().iterator();
            while (iter.hasNext()) {
                var entry = iter.next();
                ChunkPosition chunk = entry.getKey();
                // plan to use more nb lock
                AtomicInteger chunkCounter = chunkLock.computeIfAbsent(chunk, (c) -> new AtomicInteger(0));
                int chunkC = chunkCounter.getAndIncrement();
                if (chunkC == 0) {
                    var tickLocationIter = entry.getValue();
                    if (tickLocationIter.hasNext()) {
                        TickLocation tickLocation = tickLocationIter.next(); // 获取 TickLocation
                        Location location = tickLocation.getLocation(); // 获取 Location
                        tickLocationAsyncParalevel2(tickers, syncTickers, location, chunkCounter);
                    } else {
                        iter.remove();
                    }
                } else if (chunkC > 20) {
                    chunkCounter.set(0);
                }
            }
        }
    }

    private void tickLocationAsyncParalevel2(
            @Nonnull HashMap<BlockTicker, CompletableFuture<Void>> tickers,
            HashSet<BlockTicker> syncticker,
            @Nonnull Location l,
            AtomicInteger chunkCounter) {
        var blockData = StorageCacheUtils.getBlock(l);
        if (blockData == null || !blockData.isDataLoaded() || blockData.isPendingRemove()) {
            chunkCounter.set(0);
            return;
        }
        SlimefunItem item = SlimefunItem.getById(blockData.getSfId());

        if (item != null && item.getBlockTicker() != null) {
            if (item.isDisabledIn(l.getWorld())) {
                chunkCounter.set(0);
                return;
            }
            try {
                BlockTicker ticker = item.getBlockTicker();
                ticker.update();
                if (ticker.isSynchronized()) {
                    // sync task run later, so lock is not required
                    chunkCounter.set(0);
                    syncticker.add(ticker);
                    Slimefun.getProfiler().scheduleEntries(1);
                    /**
                     * We are inserting a new timestamp because synchronized actions
                     * are always ran with a 50ms delay (1 game tick)
                     */
                    Slimefun.runSync(() -> {
                        Block b = l.getBlock();
                        tickBlock(l, item, blockData, System.nanoTime());
                    });
                } else {
                    tickers.compute(ticker, (bt, future) -> {
                        Runnable tickTask = () -> {
                            tickBlockInAsync(l,ticker,item,blockData);
                        };
                        return future == null
                                ? CompletableFuture.runAsync(tickTask, getTickerThreadPool())
                                        .exceptionally((ignored) -> {
                                            chunkCounter.set(0);
                                            return null;
                                        })
                                : future.thenRunAsync(tickTask, getTickerThreadPool())
                                        .exceptionally((ignored) -> {
                                            chunkCounter.set(0);
                                            return null;
                                        });
                    });
                }
            } catch (Throwable x) {
                chunkCounter.set(0);
                reportErrors(l, item, x);
            }
        }
    }
    @ParametersAreNonnullByDefault
    protected void reportErrors(Location l, SlimefunItem item, Throwable x){
        if(!this.poolResetting){
            super.reportErrors(l, item, x);
        }
    }
}