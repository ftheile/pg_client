#include <libpq-fe.h>


int main(int argc, char* argv[])
{
	int ret = 0;
	if (argc == 2) {
		PGconn* conn;
		conn = PQconnectdb(argv[1]);
		if (PQstatus(conn) == CONNECTION_OK) {
			PGresult* res;
			res = PQexec(conn, "SELECT data_id, name, len  FROM params ORDER BY data_id");
			if (PQresultStatus(res) == PGRES_TUPLES_OK) {
				int rows = PQntuples(res);
				int cols = PQnfields(res);
				int r, c;
				// Column names:
				for (c=0; c<cols; c++) {
					printf("%s ", PQfname(res, c));
				}
				printf("\n");

				// Data:
				for (r=0; r<rows; r++) {
					for (c=0; c<cols; c++) {
						printf("%s ", PQgetvalue(res, r, c));
					}
					printf("\n");
				}
				PQclear(res);
			} else {
				printf("Error PQexec()\n");
				ret = -3;
			}
		} else {
			printf("Error PQconnectdb()\n");
			ret = -2;
		}
		PQfinish(conn);
	} else {
		printf("Usage:\n");
		printf("  %s <connection string>\n", argv[0]);
		ret = -1;
	}
	return ret;
}
