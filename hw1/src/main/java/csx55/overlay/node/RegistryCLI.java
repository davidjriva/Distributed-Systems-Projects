package csx55.overlay.node;

import java.util.Scanner;


public class RegistryCLI {
    private final Registry registry;
    private final OverlayHandler overlayHandler;

    public RegistryCLI(Registry registry) {
        this.registry = registry;
        this.overlayHandler = new OverlayHandler(registry);
    }

    public void runCLI() {
        Scanner scanner = new Scanner(System.in);
        String input = "";

        while(true) {
            input = scanner.nextLine();

            if(input.equals("list-messaging nodes")){
                listMessagingNodes();
            } else if(input.startsWith("setup-overlay")){
                overlayHandler.clearOverlay();
                setupOverlay(input);
            } else if(input.equals("list-weights")){
                overlayHandler.listWeights();
            } else if(input.equals("send-overlay-link-weights")){
                overlayHandler.sendOverlayLinkWeights();
            } else if(input.startsWith("start")){
                overlayHandler.sendStartCommand(input);
            } 
        }
    }

    public void listMessagingNodes() {
        for (String hostNameAndPort : registry.getConnectedNodes().keySet()){
            System.out.println(hostNameAndPort);
        }
    }

    public void setupOverlay(String input) {
        overlayHandler.setupOverlay(input, registry.getConnectedNodes());
    }
}