package org.apache.spark.sql.leo.exceptions;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException;

/**
 * This exception will be thrown if model is not found when executing model
 * related SQL statement
 */
@InterfaceAudience.User
@InterfaceStability.Stable
public class NoSuchExperimentException extends MalformedCarbonCommandException {

  /**
   * default serial version ID.
   */
  private static final long serialVersionUID = 1L;

  public NoSuchExperimentException(String modelName) {
    super("Experiment with name " + modelName + " does not exist");
  }
}