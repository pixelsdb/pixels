package io.pixelsdb.pixels.lambda;

import java.util.ArrayList;

public class LambdaEvent {
    ArrayList<String> fileNames;
    ArrayList<String> cols;

    public LambdaEvent(ArrayList<String> fileNames, ArrayList<String> cols) {
        this.fileNames = fileNames;
        this.cols = cols;
    }
}
