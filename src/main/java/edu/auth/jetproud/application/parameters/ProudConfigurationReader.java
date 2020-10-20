package edu.auth.jetproud.application.parameters;

import edu.auth.jetproud.application.config.ProudConfiguration;
import edu.auth.jetproud.application.parameters.errors.ProudArgumentException;
import edu.auth.jetproud.utils.Lists;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ProudConfigurationReader {

    public ProudConfiguration read(String[] args) throws ProudArgumentException {
        return read(Lists.from(args));
    }

    public ProudConfiguration read(List<String> args) throws ProudArgumentException {
        ProudConfiguration config = new ProudConfiguration();
        Map<ProudParameter, Object> configMap = new HashMap<>();

        if (args == null) {
            throw ProudArgumentException.invalidArgumentSequence();
        }

        if (args.size() % 2 != 0)
            throw ProudArgumentException.invalidArgumentSequence();

        for(int i=0; i < args.size(); i++) {
            String argSwitch = args.get(i);
            String argValue = args.get(++i);

            ProudParameter<?> parameter = ProudParameter.forSwitch(argSwitch);

            if (parameter == null)
                throw ProudArgumentException.unknownSwitch(argSwitch);

            Object value = parameter.readValue(argValue);

            if (value == null)
                throw ProudArgumentException.unreadableValue(argValue, argSwitch);

            if (value instanceof List) {
                List listValue = (List) value;

                if (listValue.stream().anyMatch(Objects::isNull))
                    throw ProudArgumentException.unreadableValue(argValue, argSwitch);
            }

            configMap.put(parameter, value);
        }

        for (ProudParameter parameter:ProudParameter.requiredParameters()) {
            if (!configMap.containsKey(parameter))
                throw ProudArgumentException.requiredSwitchMissing(parameter.getSwitchValue());
        }

        config.setAllFrom(configMap);
        config.validateConfiguration();

        return config;
    }

}
