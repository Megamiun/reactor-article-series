package br.com.gabryel.reactorarticle._0_basics

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.time.temporal.ChronoUnit.MILLIS
import java.time.temporal.ChronoUnit.SECONDS
import java.time.temporal.TemporalUnit
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

private data class User(
    val id: Int,
    val name: String,
    val age: Int,
    var lastLogin: LocalDateTime = LocalDateTime.now()
) {
    override fun toString() = """{ "id": $id, "name": "$name", "age": $age, "lastLogin": "$lastLogin" }"""
}

private val start = LocalDateTime.now()
private var showThreads = false

private val userDB = mapOf(
    1 to User(1, "Joaquim", 17),
    2 to User(2, "Antônio", 28),
    3 to User(3, "Isabella", 31),
    4 to User(4, "Gabriel", 28),
    5 to User(5, "Leticia", 10)
)

fun main() {
    fun fetchUserById(id: Int) = Mono.fromSupplier {
        log("Fetching User $id from DB", "fromSupplier")
        userDB[id] ?: User(-1, "Unknown", -1)
    }

    "Create and execute Mono Publisher" {
        fetchUserById(1)
            .subscribe { event -> log("Event Content: $event") }
    }

    "Reusing a Mono Publisher" {
        log("Let me call this guy once")
        val anUser: Mono<User> = fetchUserById(1)
        anUser.subscribe { event -> log("Ok, it is ${event.name}", "subscribe") }

        log("\nNow again, to get his age")
        val anUserAge: Mono<Int> = anUser.map { event ->
            log("Let me get his age", "map")
            event.age
        }
        anUserAge.subscribe { event -> log("Age: $event", "subscribe") }

        log("\nOh, but I only want to get people who can already drive")
        anUser.filter { event ->
            log("Filtering off ${event.name} in case it is not over 21", "filter")
            event.age >= 21
        }.doOnNext { event -> log("This guy is over 21: $event", "doOnNext") }
            .doOnSuccess { event -> log("I always get called, look at my result: $event", "doOnSuccess") }
            .subscribe { event -> log("I only get called if a event has arrived: $event", "subscribe") }
    }

    "Reading a stream of data from a Flux" {
        val multipleValues = Flux.just(1, 2, 3)

        multipleValues.flatMap { event ->
            log("\nOk, let me search for user with id $event", "flatmap")
            fetchUserById(event)
        }
            .doOnNext { event -> log("Ok, it was ${event.name}", "doOnNext") }
            .subscribe()
    }

    "Reading (potentially) infinite data stream from a Flux" {
        val maxIterations = 3
        Flux.interval(Duration.ofMillis(50))
            .takeWhile { iteration ->
                if (iteration > maxIterations) log("It has gone too long, let me turn it off", "takeUntil")
                iteration <= maxIterations
            }.flatMap { event ->
                log("\nLet me take a look at the database", "flatMap")
                fetchUserById(event.toInt())
            }.doOnNext { event -> log("Cool, I always wanted to meet ${event.name}!", "doOnNext") }
            .doOnComplete { log("\nOof, finally finished!", "doOnComplete") }
            .subscribe()

        Thread.sleep(500) // Thread.sleep(500)? What are you talking about? I don't see anything
    }

    "Creating Hot Publishers via cache()" {
        val cached = Flux.just(1, 2, 3)
            .flatMap { event ->
                log("\nOk, let me search for user with id $event", "flatmap")
                fetchUserById(event)
            }.cache()

        log("Ok, let`s do first round")
        cached.subscribe { event -> log("[1] Hey ya, ${event.name}", "subscribe") }

        log("\nAnd then a second time!")
        cached.subscribe { event -> log("[2] Hey ya, ${event.name}", "subscribe") }
    }

    "Creating a Publisher that works as a Topic" {
        val interval = Flux.interval(Duration.ofMillis(100))
            .doOnNext { event -> log("\nOk, event #$event created and sent") }
            .share()

        val intervalSubscription = interval
            .subscribe { event -> log("Event $event received by subscriber 0") }

        Thread.sleep(100)

        val allSubscriptions = listOf(
            intervalSubscription,
            interval.subscribe { event -> log("Event $event received by subscriber 1") },
            interval.subscribe { event -> log("Event $event received by subscriber 2") },
        )

        allSubscriptions.forEachIndexed { index, disposable ->
            Thread.sleep(100)
            log("Canceling subscription $index, so it don`t run forever")
            disposable.dispose()
        }

        Thread.sleep(200)

        log("\nAnd then there was none...")
    }

    showThreads = true
    val clientIO = Schedulers.newParallel("client-io", 2)
    val serverIO = Schedulers.newParallel("server-io", 2)
    val serverMain = Schedulers.newParallel("server-main", 2)

    fun ioFetchUserById(id: Int) =
        Mono.defer { fetchUserById(id) }
            .delayElement(Duration.of(400, MILLIS))
            .subscribeOn(serverIO)

    fun serverGetName(id: Int, delay: Long = 50, unit: TemporalUnit = MILLIS) =
        Mono.defer {
            log("Request received at server: getName($id)", "flatmap")
            ioFetchUserById(id)
        }.publishOn(serverMain)
        .map {
            log("Extracting name from DB request from $id", "map")
            it.name
        }.delayElement(Duration.of(delay, unit))
            .subscribeOn(serverMain)

    "Setting up parallelism" {
        log("Ok, let me setup some work here\n")

        val callsUnanswered = AtomicInteger()

        fun callServer(id: Int, delay: Long = 0, unit: ChronoUnit = MILLIS) {
            callsUnanswered.incrementAndGet()
            serverGetName(id, delay, unit)
                .publishOn(clientIO)
                .doOnSuccess { callsUnanswered.decrementAndGet() }
                .subscribe { event -> log("Username #1: $event") }
        }

        callServer(1, 1, SECONDS)
        callServer(2, 500, MILLIS)
        callServer(3)

        while (callsUnanswered.get() != 0) {
            log("Waiting for ${callsUnanswered.get()} requests to finish... But I could be doing other work!\n")
            Thread.sleep(400)
        }
    }

    "Log user logins" {
        val queue = Sinks.many().multicast().onBackpressureBuffer<Int>()
        val queueOutput = queue.asFlux()
        val eventsRemaining = AtomicInteger()

        fun serverLogin(id: Int) = Mono.fromSupplier {
            queue.emitNext(id, busyLooping(Duration.of(200, MILLIS)))
            log("User #$id is trying to login!")

            UUID.randomUUID()
        }.subscribeOn(serverMain)

        fun clientLogin(id: Int) {
            eventsRemaining.addAndGet(2)
            log("Preparing to call server for user #$id")

            serverLogin(id)
                .publishOn(clientIO)
                .map { event -> log("Token arrived for user #$id: $event") }
                .subscribe()
        }

        val loggerDisposable = queueOutput
            .delaySequence(Duration.of(10, MILLIS))
            .publishOn(serverMain)
            .subscribe { event ->
                log("User #$event has logged in!")
                eventsRemaining.decrementAndGet()
            }

        val updateDbDisposable = queueOutput
            .delaySequence(Duration.of(200, MILLIS))
            .publishOn(serverIO)
            .subscribe { event ->
                userDB[event]?.lastLogin = LocalDateTime.now()
                log("Changed user #$event lastLogin to ${userDB[event]?.lastLogin}!")
                eventsRemaining.decrementAndGet()
            }

        repeat(3) { clientLogin(it + 1) }

        while (eventsRemaining.get() != 0) Thread.sleep(100)

        loggerDisposable.dispose()
        updateDbDisposable.dispose()
    }

    "Handling errors" {
        fun getUserOrThrow(id: Int) = Mono.fromSupplier {
            userDB[id] ?: throw IllegalArgumentException("No user with id $id found")
        }

        fun getUserOrEmpty(id: Int) = Mono.justOrEmpty(userDB[id])

        log("Let me search for user #999")
        getUserOrThrow(999).subscribe { event -> log(event, "subscribe") }
        log("User #999 has not been found, even so my code continues?")

        log("\nLet`s try again")
        getUserOrThrow(999)
            .onErrorResume {
                log("I can`t find user #999, they have thrown a ${it.javaClass.simpleName}", "onErrorResume")
                getUserOrThrow(1)
            }.subscribe { event -> log("Here the final user: ${event?.name}", "subscribe") }

        log("\nAnd again")
        getUserOrEmpty(999)
            .doOnSuccess { event -> log("Here is user #999: ${event?.name}", "doOnSuccess") }
            .switchIfEmpty(
                getUserOrEmpty(1).doOnSuccess {
                    log("As no user has been found, I am switching to User #1", "switchIfEmpty")
                }
            )
            .subscribe { event -> log("Here the final user: ${event?.name}", "subscribe") }
    }

    clientIO.dispose()
    serverIO.dispose()
    serverMain.dispose()
}

private operator fun String.invoke(invoke: () -> Unit) {
    println("---------------$this---------------")

    invoke()
    println()
}

private fun log(message: Any? = "", operator: String = "", shouldPrint: Boolean = true) {
    if (!shouldPrint) return

    val time = start.until(LocalDateTime.now(), MILLIS).toString().padStart(6, '-') + "ms"
    val thread = Thread.currentThread().name.padStart(14, '-')
    val paddedOperator = operator.padStart(14, '-')
    val lines = message.toString().lines().map {
        if (it.isBlank()) it
        else if (showThreads) "[$time] [$thread] [$paddedOperator] $it"
        else "[$time] [$paddedOperator] $it"
    }

    println(lines.joinToString("\n"))
}