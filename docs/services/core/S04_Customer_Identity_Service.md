# Telcenter Core - S04: Customer Identity

Introducing the series of Telcenter Engineering.

Telcenter, on the surface, is a semi-automated telecom services call center -
it is a web app that offers telecommunication services consultation. People
are serviced by the AI Agent, and they will be forwarded to in-person
consultants if the AI detected down mood, rage, or that it could not answer
the question itself given a pre-fed ground truth database. Now, we are
designing this as microservices. Telcenter Core would act as the main backend
for the end-user interface, and it consists of multiple microservices.
Telcenter Partner is another system that is deployed separately on each of
the telecom partner's servers, and it is responsible for taking up forwarded
conversations and continuing them with the real persons in-charge. Together,
one Core and several Partner systems cooperate to deliver the best customer
experience, while lowering cost dramatically, with the help of automated AI
responses.

The general deployment and communication topology is like this:

    Core <---(Internet)---> (Partner_1, Partner_2..., Partner_N)

The users' inquiries and answers to those are primarily in Vietnamese.

Now, you are designing the S04: Customer Identity service, in Python.
This service is inside the TELCENTER CORE system.

Here are the peer services that the S04: Customer Identity service may interact with. We will come up
with the flow of this service itself later.

- S08_METRICS SERVICE is the service responsible for collecting, storing, processing, and providing statistical data (metrics) for the entire Telcenter Core system.

## A Note on API Transport Layers

The APIs of the services (including this one
and the peers) might be based on HTTP and/or
RabbitMQ transport protocols. One service might
also exposes multiple APIs of different kinds.

HTTP is mostly used in APIs that are exposed
to the frontend web apps, though it occasionally
is used for internal communication between
microservices, too. HTTP APIs are somewhat
RESTful (it is CRUD, stateless, versioned,
and HATEOAS, but it need not follow
Code-on-Demand requirements.)

For APIs that are based on RabbitMQ transport,
each API usually demands two queues, the
requests queue and the responses queue. The
caller would send requests into the former queue
and expect the responses to come out from the
latter. Exceptions will be explicitly noted.
The default queue names will be specified for
each such API. The queue names should be configurable
via `.env`, too.

## Peer Service APIs

Note that the base URL to call the services
must be specified via `.env`. Construct
a `.env.example` file for that.

### S08 Metrics Service

[A04](../../api_groups/A04.md)

## Database Design

S04 sử dụng MongoDB với collection chính sau:

### Collection: `customers`

**Schema:**

```json
{
  "_id": ObjectId,
  "phone_number": String,  // Unique
  "password_hash": String,
  "full_name": String,
  "address": String,
  "created_at": Date,
  "status": String
}
```

**Indexes:** `{ "phone_number": 1 }` (unique)

Cấu hình DB: `MONGO_URL` trong `.env`.

## The Flow

Based on the APIs' behavior.

## This Service's APIs

[H20](../../api_groups/H20.md)

## Configuration Required

This endpoint requires the following Google OAuth environment variables:

  GOOGLE_CLIENT_ID
  GOOGLE_CLIENT_SECRET
  GOOGLE_REDIRECT_URI

Be sure to include them in .env.example
and provide the instructions on where
to get them.

## Technology

- Python
- Use `uv` as the virtual environment and package manager.
- Multithreaded logic should be used for performance, since this
    component relies a lot on other services, which means the API calls
    to those services take up very much time. So this service is I/O bound.
    Note that, using multithreading to emulate async operations is very
    important - but do NOT use `async` and `await` in Python - that would
    be a mess!

- The class `MessageQueueService` must be used for RabbitMQ communication (which internally
    use `pika`).

    The class is [located in this file](../../../app/services/MessageQueueService.py).

    An example of using this class [is given here](../../MessageQueueService-usage-example.py).

    Also, for multithreading, only use the scheme in that file.
    Any other use of multithreading, if necessary, must strictly
    look for hazards - use locks and other synchronization primitives
    where appropriate.

- If this service needs to expose HTTP API(s), use Flask. For CRUD tasks, the class
    `BaseCRUDService` must be subclassed, overriding appropriately. The class
    is [located in this file](../../../app/services/common/BaseCRUDService.py)

- The program entry point is [in this file](../../../app/__main__.py).

- Embraces Dependency Injection practices
- All actions are logged for **audit & security purposes**. Currently,
    there must be a class dedicated for logging, and its instances
    are injectible to the service classes that need logging.
