import java.util.ArrayList;
import java.util.regex.Pattern;

public class Robots_txt {
    private static final String StartPattern = "Disallow: ";
    private ArrayList<Pattern> patterns_ = new ArrayList<>();

    public Robots_txt() {}

    public Robots_txt(String robots_txt) {
        if (robots_txt.length() == 0) return;

        String[] rules = robots_txt.split("\n");

        for (String rule_str: rules) {
            rule_str = rule_str.substring(StartPattern.length());

            if (rule_str.startsWith("/")) {
                rule_str = "^\\Q"+rule_str+"\\E.*";
            } else if (rule_str.startsWith("*")) {
                rule_str = rule_str.substring(1); 
                rule_str = ".*\\Q"+rule_str+"\\E.*";
            } else if (rule_str.endsWith("$")) {
                    rule_str = ".*\\Q"+rule_str+"\\E$";
            }
            if (rule_str.endsWith("$\\E.*")) 
                rule_str = rule_str.substring(0, rule_str.length()-5)+"\\E$";
            

            patterns_.add(Pattern.compile(rule_str));
        }
    }

    public boolean IsAllowed(String url_str) {
        for (Pattern p: patterns_) {
            if (p.matcher(url_str).matches()) {
                return false;
            }
        }

        return true;
    }
}