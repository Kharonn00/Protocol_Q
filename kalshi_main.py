import os
import sys
import asyncio
import logging
import gc
from aiohttp import web

from engine import (
    config, BotConfig, GLOBAL_SSL_CONTEXT,
    SimExecutionBroker, LiveKalshiBroker, LiveTradingEngine,
    coinbase_websocket_consumer, market_worker_loop, binance_websocket_consumer,
    bybit_websocket_consumer, hyperliquid_websocket_consumer, kalshi_websocket_consumer,
    binance_worker_loop, get_kalshi_credentials, handle_health_check, log_exception_group
)

# Optimize Garbage Collection thresholds to reduce latency jitter in high-frequency trading loops
gc.set_threshold(7000, 10, 10)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
logger = logging.getLogger("KalshiQuantEngine")

async def start_health_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    app.router.add_get('/health', handle_health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", "8080"))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.debug(f"[HEALTH SERVER] Micro HTTP health responder online, listening on port {port}")
    return runner

# Lock all module-level imports, classes, and configurations into the permanent 
# GC generation to prevent the garbage collector from scanning them during hot loops.
gc.freeze()

# ==========================================
# BOOTSTRAPPER
# ==========================================
if __name__ == "__main__":
    async def main():
        env_mode = os.environ.get("BOT_ENV", "simulation").lower()
        
        if env_mode in ["live", "paper"]:
            key_id, private_key = get_kalshi_credentials("prod/kalshi/api-keys", region_name="us-east-1")
            if env_mode == "live":
                confirm = os.environ.get("LIVE_TRADING_CONFIRM", "")
                if confirm != "I_ACCEPT_FINANCIAL_RISK":
                    logger.critical("Live mode blocked. Halting.")
                    sys.exit(1)
                logger.warning("!!! INITIALIZING LIVE TRADING BROKER !!!")
                broker = LiveKalshiBroker(key_id=key_id, private_key=private_key, paper_trade=False)
            else:
                logger.debug("Initializing PAPER TRADING Broker.")
                broker = LiveKalshiBroker(key_id=key_id, private_key=private_key, paper_trade=True)
            key_id = None
            private_key = None
            del private_key
            del key_id
            gc.collect()
        else:
            logger.debug("Initializing SIMULATION Broker.")
            broker = SimExecutionBroker()

        await broker.start()
        engine = LiveTradingEngine(broker)
        
        if sys.platform != "win32":
            import signal
            loop = asyncio.get_running_loop()
            def _on_sigterm():
                logger.warning("SIGTERM received from OS. Initiating shutdown...")
                engine.shutting_down = True
                for task in asyncio.all_tasks(loop):
                    name = task.get_name()
                    if name and ("consumer" in name or "worker" in name or "sync" in name):
                        task.cancel()
            loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
        
        health_runner = None
        try:
            tick_queue = asyncio.Queue(maxsize=10000) 
            binance_queue = asyncio.Queue(maxsize=1000)
            
            health_runner = await start_health_server()
            
            async with asyncio.TaskGroup() as tg:
                tg.create_task(engine.sync_balance_loop(), name="sync_balance")
                tg.create_task(engine.sync_markets_loop(), name="sync_markets")
                tg.create_task(engine.sync_macro_calendar_loop(), name="sync_macro_calendar")
                tg.create_task(engine.sync_macro_trend_loop(), name="sync_macro_trend")
                tg.create_task(coinbase_websocket_consumer(engine, tick_queue), name="consumer")
                tg.create_task(market_worker_loop(engine, tick_queue), name="worker")
                tg.create_task(binance_websocket_consumer(engine, binance_queue), name="binance_consumer")
                tg.create_task(bybit_websocket_consumer(engine, binance_queue), name="bybit_consumer")
                tg.create_task(hyperliquid_websocket_consumer(engine, binance_queue), name="hyperliquid_consumer")
                tg.create_task(binance_worker_loop(engine, binance_queue), name="binance_worker")
                
                if isinstance(broker, LiveKalshiBroker):
                    if broker.paper_trade:
                        tg.create_task(engine.paper_fill_dispatcher(), name="paper_fill_dispatcher")
                    else:
                        tg.create_task(kalshi_websocket_consumer(engine), name="kalshi_consumer")
                
        except Exception as e:
            log_exception_group(e)
        finally:
            logger.debug("Executing final engine shutdown protocols...")
            if health_runner:
                try:
                    await health_runner.cleanup()
                except Exception as ex:
                    logger.warning(f"Error during health server cleanup: {ex}")
            await engine.shutdown()
            await broker.close()

    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        logger.debug("Bot halted manually.")