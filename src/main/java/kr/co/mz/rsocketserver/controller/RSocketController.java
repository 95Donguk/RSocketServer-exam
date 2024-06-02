package kr.co.mz.rsocketserver.controller;

import java.time.Duration;
import kr.co.mz.rsocketserver.dto.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

@Controller
@Slf4j
public class RSocketController {

  @MessageMapping("request-response")
  public Message requestResponse(Message request) {
    log.info("Received request-response request: {}", request);
    return new Message("mega-server", "responder");
  }

  @MessageMapping("stream")
  Flux<Message> stream(Message request) {
    log.info("Received stream request: {}", request);
    return Flux.interval(Duration.ofSeconds(1))
        .map(index -> new Message("mega-server", "responder"));
  }

  @MessageMapping("fire-and-forget")
  public void fireAndForget(Message request) {
    log.info("Received fire-and-forget request: {}", request);
  }

  @MessageMapping("channel")
  Flux<Message> channel(final Flux<Duration> settings) {
    log.info("settinsg : {}", settings);
    return settings.doOnNext(setting -> log.info("Frequency setting is {} second(s).", setting.getSeconds()))
        .switchMap(setting -> Flux.interval(setting)
            .map(index -> new Message("mega-server", "responder"))
        );
  }
}
