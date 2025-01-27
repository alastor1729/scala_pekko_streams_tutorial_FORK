package actor;

import org.apache.pekko.actor.typed.*;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;

/**
 * Akka Typed example 1:1
 * Doc:
 * https://doc.akka.io/docs/akka/current/typed/actors.html
 *
 */
public class HelloWorldMain extends AbstractBehavior<HelloWorldMain.Start> {

    public static void main(String[] args) throws Exception {
        final ActorSystem<HelloWorldMain.Start> system =
                ActorSystem.create(HelloWorldMain.create(), "hello");

        system.tell(new HelloWorldMain.Start("World"));
        system.tell(new HelloWorldMain.Start("Akka"));

        Thread.sleep(3000);
        system.terminate();
    }


    // Start message...
    public static class Start {
        public final String name;

        public Start(String name) {
            this.name = name;
        }
    }

    public static Behavior<Start> create() {
        return Behaviors.setup(HelloWorldMain::new);
    }

    private final ActorRef<HelloWorld.Greet> greeter;

    private HelloWorldMain(ActorContext<Start> context) {
        super(context);

        final String dispatcherPath = "akka.actor.default-blocking-io-dispatcher";
        Props greeterProps = DispatcherSelector.fromConfig(dispatcherPath);
        greeter = getContext().spawn(HelloWorld.create(), "greeter", greeterProps);
    }

    @Override
    public Receive<Start> createReceive() {
        return newReceiveBuilder().onMessage(Start.class, this::onStart).build();
    }

    private Behavior<Start> onStart(Start command) {
        ActorRef<HelloWorld.Greeted> replyTo =
                getContext().spawn(HelloWorldBot.create(3), command.name);
        greeter.tell(new HelloWorld.Greet(command.name, replyTo));
        return this;
    }
}