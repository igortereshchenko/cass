from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra import ReadTimeout
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.policies import WhiteListRoundRobinPolicy, ConstantReconnectionPolicy
import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from datetime import datetime
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

cloud_config= {
        'secure_connect_bundle': 'secure-connect-elko.zip'
}
auth_provider = PlainTextAuthProvider('elko', 'elkoelko')

profile = ExecutionProfile(

    retry_policy=ConstantReconnectionPolicy(delay=10),
    request_timeout=15

)



cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider, execution_profiles={'EXEC_PROFILE_DEFAULT': profile})
session = cluster.connect()

row = session.execute("select release_version from system.local").all()
if row:
    print(row[0])
else:
    print("An error occurred.")


# Executing Queries
session.set_keyspace('elko')
rows = session.execute('SELECT * FROM "Team_Members"')
for data in rows:
    print (data.team_name, data.member_name)


# INSERT INTO "Team_Members" (team_name, team_location, team_manager, member_name, member_nationality, member_position)
# VALUES ('Team 2', 'Lviv', 'Boby Jones', 'Kate', 'Spain', 'cute girl');
session.execute(
    """
    INSERT INTO "Team_Members" (team_name, team_location, team_manager, member_name, member_nationality, member_position)
    VALUES (%(team_name)s, %(team_location)s, %(team_manager)s, %(member_name)s, %(member_nationality)s, %(member_position)s)
    """,
    {'team_name': "Bobs Team", 'team_location': 'Kyiv', 'team_manager': 'Bob', 'member_name': 'Bob', 'member_nationality': 'Ukraine', 'member_position': 'Big boss'}
)

# The driver supports asynchronous query execution through execute_async()

query = 'SELECT * FROM "Team_Members" '
future = session.execute_async(query )

print(' ... do some other work')

try:
    rows = future.result()
    user = rows[0]
    print(data.team_name, data.member_name)
except ReadTimeout:
    print("Query timed out:")


# This works well for executing many queries concurrently:


# build a list of futures
futures = []
query = 'SELECT * FROM "Team_Members" '
for i in range(10):
    futures.append(session.execute_async(query))


# wait for them to complete and use the results
for future in futures:
    rows = future.result()
    print (rows[0].team_name)


# Alternatively, instead of calling result(), you can attach callback and errback functions
# through the add_callback(), add_errback(), and add_callbacks(), methods.
# If you have used Twisted Python before, this is designed to be a lightweight version of that:

def handle_success(rows):

    try:
        data = rows[0]
        # todo something with data
        print('handle_success',data.team_name)
    except Exception:
        print("Failed to process data %s", data)
        # don't re-raise errors in the callback

def handle_error(exception):
   print("Failed to fetch user info: %s", exception)

future = session.execute_async(query)
future.add_callbacks(handle_success, handle_error)
# There are a few important things to remember when working with callbacks:
# Exceptions that are raised inside the callback functions will be logged and then ignored.
#
# Your callback will be run on the event loop thread, so any long-running operations will prevent other requests from being handled




batch = BatchStatement()
# batch.add(SimpleStatement("""
#     INSERT INTO "Team_Members" (team_name, team_location, team_manager, member_name, member_nationality, member_position)
#     VALUES (%(team_name)s, %(team_location)s, %(team_manager)s, %(member_name)s, %(member_nationality)s, %(member_position)s)
#     """,
#     {'team_name': "Bobs Team 2", 'team_location': 'Kyiv', 'team_manager': 'Bob', 'member_name': 'Bob', 'member_nationality': 'Ukraine', 'member_position': 'Big boss'}))
#
# batch.add(SimpleStatement("""
#     INSERT INTO "Team_Members" (team_name, team_location, team_manager, member_name, member_nationality, member_position)
#     VALUES (%(team_name)s, %(team_location)s, %(team_manager)s, %(member_name)s, %(member_nationality)s, %(member_position)s)
#     """,
#     {'team_name': "Bobs Team 3", 'team_location': 'Kyiv', 'team_manager': 'Bob', 'member_name': 'Bob', 'member_nationality': 'Ukraine', 'member_position': 'Big boss'}).setIdempotent(True))
session.execute(batch)



class ExampleModel(Model):
    __keyspace__ = 'elko'
    __connection__ = session
    example_id      = columns.UUID(primary_key=True, default=uuid.uuid4)
    example_type    = columns.Integer(index=True)
    created_at      = columns.DateTime()
    description     = columns.Text(required=False)

#next, setup the connection to your cassandra server(s)...
# see http://datastax.github.io/python-driver/api/cassandra/cluster.html for options
# the list of hosts will be passed to create a Cluster() instance


#...and create your CQL table
sync_table(ExampleModel)

#now we can create some rows:
em1 = ExampleModel.create(example_type=0, description="example1", created_at=datetime.now())
em2 = ExampleModel.create(example_type=0, description="example2", created_at=datetime.now())
em3 = ExampleModel.create(example_type=0, description="example3", created_at=datetime.now())
em4 = ExampleModel.create(example_type=0, description="example4", created_at=datetime.now())
em5 = ExampleModel.create(example_type=1, description="example5", created_at=datetime.now())

#and now we can run some queries against our table
ExampleModel.objects.count()

q = ExampleModel.objects(example_type=1)
print(q.count())

for instance in q:
     print (instance.description)

#here we are applying additional filtering to an existing query
#query objects are immutable, so calling filter returns a new
#query object
q2 = q.filter(example_id=em5.example_id)

for instance in q2:
    print (instance.description)
