#include <libpq-fe.h>

PGconn* conn;

int main(int argc, char* argv[])
{
	int ret = 0;
	if (argc == 2) {
		conn = PQconnectdb(argv[1]);
		if (PQstatus(conn) == CONNECTION_OK) {
			// ...
		} else {
			printf("Error PQstatus()\n");
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
