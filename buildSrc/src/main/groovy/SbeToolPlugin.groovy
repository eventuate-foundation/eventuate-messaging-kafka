import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.JavaExec

class SbeToolPlugin implements Plugin<Project> {

    void apply(Project project) {
        def sbeTask = project.task("sbeTool", type: SbeToolTask) {
        }

        project.sourceSets.main.java.srcDirs += [sbeTask.generatedDir]

        project.getTasks().getByPath("compileJava").dependsOn(sbeTask)
    }
}