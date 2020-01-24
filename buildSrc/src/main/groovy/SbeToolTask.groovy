import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction
import uk.co.real_logic.sbe.SbeTool

class SbeToolTask extends DefaultTask{

    @TaskAction
    def generate() {
        def args = new String[1]
        args[0] = "${project.projectDir}/src/main/resources/sbe-schema.xml" as String
        System.setProperty("sbe.output.dir",  generatedDir)
        System.setProperty("sbe.target.namespace", "io.eventuate.messaging.kafka.common.sbe" )
        SbeTool.main(args)
    }

    def getGeneratedDir() {
        "${project.buildDir}/generated-sources/sbe"
    }
}
