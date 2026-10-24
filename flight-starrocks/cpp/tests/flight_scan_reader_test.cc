#include "flight_starrocks/flight_scan_reader.h"

#include <arrow/api.h>
#include <arrow/flight/server.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>


namespace {

const char kToken[] = "secret-token-value";

std::shared_ptr<arrow::RecordBatch> MakeBatch(int64_t first, int64_t count) {
  arrow::Int64Builder builder;
  for (int64_t i = 0; i < count; ++i) {
    if (!builder.Append(first + i).ok()) return nullptr;
  }
  auto array = builder.Finish().ValueOrDie();
  return arrow::RecordBatch::Make(arrow::schema({arrow::field("v", arrow::int64())}),
                                  count, {array});
}

// Serves "two" (two batches), "empty" (no batches) and rejects every other ticket.
class TestServer : public arrow::flight::FlightServerBase {
 public:
  arrow::Status DoGet(const arrow::flight::ServerCallContext&,
                      const arrow::flight::Ticket& request,
                      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override {
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    if (request.ticket == "two") {
      batches = {MakeBatch(0, 3), MakeBatch(3, 2)};
    } else if (request.ticket != "empty") {
      return arrow::Status::KeyError("unknown ticket");
    }
    auto schema = arrow::schema({arrow::field("v", arrow::int64())});
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make(batches, schema));
    *stream = std::make_unique<arrow::flight::RecordBatchStream>(reader);
    return arrow::Status::OK();
  }
};

bool Fail(const std::string& message) {
  std::cerr << "FAILED: " << message << std::endl;
  return false;
}

bool CheckPullReader() {
  TestServer server;
  auto location = arrow::flight::Location::ForGrpcTcp("127.0.0.1", 0).ValueOrDie();
  arrow::flight::FlightServerOptions options(location);
  if (!server.Init(options).ok()) return Fail("server init");
  std::thread serving([&server] { (void)server.Serve(); });
  auto origin =
      arrow::flight::Location::ForGrpcTcp("127.0.0.1", server.port()).ValueOrDie();
  bool ok = true;
  {
    auto reader = flight_starrocks::FlightScanReader::Connect(origin, kToken).ValueOrDie();

    auto two = reader->OpenTicket(arrow::flight::Ticket{"two"});
    if (!two.ok()) {
      ok = Fail("open two");
    } else {
      auto stream = std::move(two).ValueOrDie();
      int64_t rows = 0;
      int batches = 0;
      while (true) {
        auto batch = stream->Next();
        if (!batch.ok()) { ok = Fail("next two"); break; }
        if (batch.ValueOrDie() == nullptr) break;
        rows += batch.ValueOrDie()->num_rows();
        ++batches;
      }
      if (rows != 5 || batches != 2) ok = Fail("two stream shape");
    }

    auto empty = reader->OpenTicket(arrow::flight::Ticket{"empty"});
    if (!empty.ok()) {
      ok = Fail("open empty");
    } else {
      auto first = std::move(empty).ValueOrDie()->Next();
      if (!first.ok() || first.ValueOrDie() != nullptr) ok = Fail("empty stream ends");
    }

    // A rejected ticket must fail without echoing the ticket or the token.
    auto bad = reader->OpenTicket(arrow::flight::Ticket{"super-secret-ticket"});
    std::string error;
    if (!bad.ok()) {
      error = bad.status().ToString();
    } else {
      auto next = std::move(bad).ValueOrDie()->Next();
      if (next.ok()) ok = Fail("bad ticket accepted");
      else error = next.status().ToString();
    }
    if (error.find("super-secret-ticket") != std::string::npos ||
        error.find(kToken) != std::string::npos) {
      ok = Fail("error leaks ticket or token: " + error);
    }
  }
  (void)server.Shutdown();
  serving.join();
  return ok;
}

}  // namespace

int main() {
  if (!CheckPullReader()) {
    return 1;
  }
  auto origin = arrow::flight::Location::ForGrpcTcp("localhost", 8815).ValueOrDie();
  arrow::flight::FlightEndpoint endpoint;
  if (flight_starrocks::EndpointLocation(origin, endpoint).ToString() !=
      origin.ToString()) {
    return 1;
  }

  endpoint.locations.push_back(arrow::flight::Location::ReuseConnection());
  if (flight_starrocks::EndpointLocation(origin, endpoint).ToString() !=
      origin.ToString()) {
    return 1;
  }
  endpoint.locations.clear();

  endpoint.locations.push_back(
      arrow::flight::Location::ForGrpcTls("worker.example.org", 8816).ValueOrDie());
  if (flight_starrocks::EndpointLocation(origin, endpoint).ToString() !=
      endpoint.locations.front().ToString()) {
    return 1;
  }

  auto reader = flight_starrocks::FlightScanReader::Connect(origin, "").ValueOrDie();
  arrow::flight::Ticket ticket;
  if (reader->ScanTicket(ticket, {}).ok()) {
    return 1;
  }
  if (reader->Scan("", [](const arrow::RecordBatch&) {
        return arrow::Status::OK();
      }).ok()) {
    return 1;
  }

  const char* host = std::getenv("FLIGHT_IT_HOST");
  const char* port = std::getenv("FLIGHT_IT_PORT");
  const char* query = std::getenv("FLIGHT_IT_QUERY");
  if (host != nullptr || port != nullptr || query != nullptr) {
    if (host == nullptr || port == nullptr || query == nullptr) {
      return 1;
    }
    auto live_location = arrow::flight::Location::ForGrpcTcp(
        host, static_cast<int>(std::stoi(port))).ValueOrDie();
    auto live_reader = flight_starrocks::FlightScanReader::Connect(live_location, "");
    if (!live_reader.ok()) {
      return 1;
    }
    int64_t rows = 0;
    auto status = live_reader.ValueOrDie()->Scan(query,
        [&rows](const arrow::RecordBatch& batch) {
          rows += batch.num_rows();
          return arrow::Status::OK();
        });
    if (!status.ok() || rows == 0) {
      return 1;
    }
  }
  return 0;
}
