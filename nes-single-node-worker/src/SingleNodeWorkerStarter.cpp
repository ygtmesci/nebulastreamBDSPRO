#include <csignal>
#include <semaphore>

#include <Configurations/Util.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>

namespace
{
std::binary_semaphore shutdownBarrier{0};

void signalHandler(int signal)
{
    NES_INFO("Received signal {}. Shutting down.", signal);
    shutdownBarrier.release();
}

NES::Thread shutdownHook(grpc::Server& server)
{
    return {
        "shutdown-hook",
        [&]()
        {
            shutdownBarrier.acquire();
            server.Shutdown();
        }};
}
}

int main(int argc, const char* argv[])
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);

        std::signal(SIGINT, signalHandler);
        std::signal(SIGTERM, signalHandler);

        auto configuration =
            NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(argc, argv);
        if (!configuration)
        {
            return 0;
        }

        const auto workerId =
            NES::WorkerId(configuration->connection.getValue().toString());

        NES::Thread::initializeThread(workerId, "main");

        NES::SingleNodeWorker worker(*configuration, workerId);

        NES::GRPCServer grpcService{std::move(worker)};

        grpc::ServerBuilder builder;
        builder.SetMaxMessageSize(-1);
        builder.AddListeningPort(
            configuration->grpcAddressUri.getValue().toString(),
            grpc::InsecureServerCredentials()
        );
        builder.RegisterService(&grpcService);
        grpc::EnableDefaultHealthCheckService(true);

        const auto server = builder.BuildAndStart();
        const auto hook = shutdownHook(*server);

        NES_INFO("Server listening on {}", configuration->grpcAddressUri.getValue());
        server->Wait();
        NES_INFO("GRPC Server shutdown");

        NES::Logger::getInstance()->forceFlush();
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
