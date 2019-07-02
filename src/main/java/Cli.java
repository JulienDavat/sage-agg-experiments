import picocli.CommandLine;

@CommandLine.Command(name = "sage-sparql-void",
        footer = "Copyright(c) 2019",
        synopsisSubcommandLabel = "COMMAND",
        description = "Generation of VoID description for Sage Endpoint(s)/Dataset(s)",
        subcommands = {SageDataset.class, SageEndpoint.class},
        mixinStandardHelpOptions = true)
public class Cli implements Runnable {
    public static void main(String... args) {
        try {
            new CommandLine(new Cli()).execute(args);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;
    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
