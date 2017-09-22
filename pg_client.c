#include <libpq-fe.h>
#include <string.h>
#include <time.h>
#ifdef BINARY_TRANSFER
#include <arpa/inet.h>
#endif
#include <stdlib.h>

#define ARG_CNT 3

int main(int argc, char* argv[])
{
	int ret = 0;
	if (argc == 3) {
		PGconn* conn;
		conn = PQconnectdb(argv[1]);
		if (PQstatus(conn) == CONNECTION_OK) {
			PGresult* res;
			ExecStatusType est;

			// Create prepared statement
			res = PQprepare(conn, "add_value", "INSERT INTO results_float (ppid, ts, value) VALUES ($1, $2, $3)", 0, NULL);
			est = PQresultStatus(res); 
			PQclear(res);
			if (est == PGRES_COMMAND_OK) {
				// Execute the prepared statement:
				int data_id = 1;
				char ts[32];
				time_t t = time(NULL);
				strftime(ts, sizeof(ts), "%F %T", localtime(&t));

#ifdef BINARY_TRANSFER
				// nbo_...: Temporary variables in network-byte-order
				float value = atof(argv[2]);
				uint32_t nbo_value = htonl(*(uint32_t*)(&value));
				uint32_t nbo_data_id = htonl(data_id);

				const char* paramValues[ARG_CNT] = { (char*)&nbo_data_id, ts, (char*)&nbo_value };
				const int paramLengths[ARG_CNT] = { sizeof(nbo_data_id), 0, sizeof(nbo_value) };
				const int paramFormats[ARG_CNT] = { 1, 0, 1 };
#else
				char str_data_id[16];
				sprintf(str_data_id, "%d", data_id);

				const char* paramValues[ARG_CNT] = { str_data_id, ts, argv[2] };
				#define paramFormats NULL
				#define paramLengths NULL
#endif
				res = PQexecPrepared(conn, "add_value", ARG_CNT, paramValues, paramLengths, paramFormats, 0);
				est = PQresultStatus(res); 
				PQclear(res);
				if (est != PGRES_COMMAND_OK) {
					printf(PQerrorMessage(conn));
					ret = -4;
				}
			} else {
				printf(PQerrorMessage(conn));
				ret = -3;
			}
		} else {
			printf(PQerrorMessage(conn));
			ret = -2;
		}
		PQfinish(conn);
	} else {
		printf("Usage:\n");
		printf("  %s <connection string> <value>\n", argv[0]);
		ret = -1;
	}
	return ret;
}
