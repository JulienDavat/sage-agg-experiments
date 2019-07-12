import picocli.CommandLine;

@CommandLine.Command(name = "sage-sparql-void",
        footer = "Copyright(c) 2019 GRALL Arnaud",
        synopsisSubcommandLabel = "COMMAND",
        description = "Generation of VoID description for Sage Endpoint(s)/Dataset(s)",
        subcommands = {SportalSparqlEndpoint.class, SparqlEndpoint.class, SageJena.class, SageDataset.class, SageEndpoint.class},
        mixinStandardHelpOptions = true)
public class Cli implements Runnable {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    public static void main(String... args) {
        try {
            new CommandLine(new Cli()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
