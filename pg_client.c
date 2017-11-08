#include <libpq-fe.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>

#ifdef BINARY_TRANSFER
	#if ((defined _WIN64) || (defined _WIN32))
		#include <winsock2.h>
	#else
		#include <arpa/inet.h>
	#endif
#endif

#define ARG_CNT 4

int main(int argc, char* argv[])
{
	int ret = 0;
	if (argc == ARG_CNT + 1) {
		PGconn* conn;
		conn = PQconnectdb(argv[1]);
		if (PQstatus(conn) == CONNECTION_OK) {
			PGresult* res;
			ExecStatusType status;

			/* Create prepared statement.
			 * Parameters:
			 *   $1: GENI destination address of pump (e.g. "35")
			 *   $2: Name of data ID (e.g. "eData_ManuelFlow")
			 *   $3: Timestamp in ISO 8601 format (e.g. "YYYY-MM-DD hh:mm:ss")
			 *   $4: Value (e.g. "3.14")
			 */
			res = PQprepare(conn, "insert_float", "INSERT INTO results_float (ppid, ts, value) SELECT ppid, $3, $4 FROM get_pump_params WHERE dest_addr = $1 AND name = $2", ARG_CNT, NULL);
			status = PQresultStatus(res); 
			PQclear(res);
			if (status == PGRES_COMMAND_OK) {
				// Execute the prepared statement:
				char ts[32];
				time_t t = time(NULL);
				strftime(ts, sizeof(ts), "%F %T", localtime(&t));

#ifdef BINARY_TRANSFER
				// Convert binary data to network byte-order:
				int dest_addr = htonl(atoi(argv[2]));
				float v = atof(argv[4]);
				unsigned int value = htonl(*(uint32_t*)&v);

				const char* paramValues[ARG_CNT] = {
					(const char*)&dest_addr,  // $1: dest_addr
					argv[3],                  // $2: data_id
					ts,						  // $3: ts
					(const char*)&value		  // $4: value
				};

				const int paramLengths[ARG_CNT] = {
					sizeof(dest_addr),  // $1
					0,                  // $2
					0,                  // $3
					sizeof(value)       // $4
				};

				const int paramFormats[ARG_CNT] = {
					1,  // $1
					0,  // $2
					0,  // $3
					1   // $4
				};
#else
				const char* paramValues[ARG_CNT] = {
					argv[2],  // $1: dest_addr
					argv[3],  // $2: data_id
					ts,       // $3: ts
					argv[4]   // $4: value
				};
				#define paramFormats NULL
				#define paramLengths NULL
#endif
				res = PQexecPrepared(conn, "insert_float", ARG_CNT, paramValues, paramLengths, paramFormats, 0);
				status = PQresultStatus(res); 
				PQclear(res);
				if (status != PGRES_COMMAND_OK) {
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
		printf("  %s <connection string> <dest_addr> <data_id> <value>\n", argv[0]);
		ret = -1;
	}
	return ret;
}
