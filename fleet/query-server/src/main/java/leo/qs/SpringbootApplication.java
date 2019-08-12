package leo.qs;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = SolrAutoConfiguration.class)
@ComponentScan(basePackages = "leo.qs")
public class SpringbootApplication extends SpringBootServletInitializer
    implements EmbeddedServletContainerCustomizer {
  @Override
  protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
    return builder.sources(SpringbootApplication.class);
  }

  @Override
  public void customize(ConfigurableEmbeddedServletContainer container) {
    container.setPort(Integer.parseInt(
        Main.getSession().conf().get("leo.leader.endpoint.port", "8080")));
  }
}