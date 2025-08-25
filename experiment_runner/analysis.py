"""
Statistical Analysis - Calculate p-values, confidence intervals, and significance classification
"""

import numpy as np
import yaml
import os
from scipy import stats
from .results_parser import ExperimentMetric

class ExperimentAnalysis:
    """Statistical analysis for experiment metrics"""
    
    def __init__(self):
        """Initialize with metrics metadata"""
        self.metrics_metadata = self._load_metrics_metadata()
    
    def _load_metrics_metadata(self):
        """Load metrics metadata from YAML file"""
        try:
            # Get the path to the data_models directory relative to this file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            metadata_path = os.path.join(current_dir, "..", "data_models", "metrics_metadata.yaml")
            
            with open(metadata_path, 'r') as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"Warning: Could not load metrics metadata: {e}")
            return {}
    
    def _get_metric_desired_direction(self, template_name: str, metric_name: str):
        """Get the desired direction for a metric from metadata"""
        if not self.metrics_metadata or 'templates' not in self.metrics_metadata:
            return None
            
        # Extract main template name and subcategory from template_name
        # e.g., 'onboarding_topline' -> template='onboarding', subcategory='topline'
        if '_' in template_name:
            main_template, subcategory = template_name.split('_', 1)
        else:
            main_template = template_name
            subcategory = None
        
        # Look in the specific template and subcategory
        if main_template in self.metrics_metadata['templates']:
            template_data = self.metrics_metadata['templates'][main_template]
            
            if subcategory and subcategory in template_data:
                subcategory_data = template_data[subcategory]
                if metric_name in subcategory_data:
                    return subcategory_data[metric_name].get('desired_direction')
        
        # Fallback: search through all templates and subcategories for the metric
        for template_name_fallback, template_data in self.metrics_metadata['templates'].items():
            for subcategory_name, subcategory_data in template_data.items():
                if subcategory_name in ['description', 'primary_business_goal', 'typical_experiments']:
                    continue  # Skip template-level metadata
                
                if metric_name in subcategory_data:
                    return subcategory_data[metric_name].get('desired_direction')
        
        return None
    
    def calculate_statistics(self, metric: ExperimentMetric):
        """Calculate p-value, confidence intervals, and statistical power"""
        
        if metric.metric_type == 'rate':
            self._calculate_rate_statistics(metric)
        elif metric.metric_type == 'continuous':
            self._calculate_continuous_statistics(metric)
        
        # Calculate absolute difference regardless of metric type
        if metric.treatment_value is not None and metric.control_value is not None:
            # Convert to float to handle decimal.Decimal types from Snowflake
            treatment_val = float(metric.treatment_value)
            control_val = float(metric.control_value)
            metric.absolute_difference = treatment_val - control_val
        
        # Calculate statistical power
        self.calculate_statistical_power(metric)
    
    def _calculate_rate_statistics(self, metric: ExperimentMetric):
        """Two-proportion z-test for rate metrics"""
        
        # Ensure we have required data
        if not all([metric.treatment_numerator is not None, metric.treatment_denominator,
                   metric.control_numerator is not None, metric.control_denominator]):
            return
        
        # Convert to numeric types (handle decimal.Decimal from Snowflake)
        try:
            x1 = float(metric.treatment_numerator)  # treatment successes
            n1 = float(metric.treatment_denominator)  # treatment total
            x2 = float(metric.control_numerator)  # control successes
            n2 = float(metric.control_denominator)  # control total
        except (ValueError, TypeError) as e:
            print(f"Error converting rate statistics to float: {e}")
            return
        
        # Check for edge cases where statistical tests are not meaningful
        total_events = x1 + x2
        min_sample_size = min(n1, n2)
        
        # If both groups have 0 events, no meaningful difference to test
        if total_events == 0:
            metric.p_value = 1.0  # No significant difference
            metric.confidence_interval_lower = 0.0
            metric.confidence_interval_upper = 0.0
            # Set values for completeness
            metric.treatment_value = 0.0
            metric.control_value = 0.0
            return
        elif total_events < 5 or min_sample_size < 10:
            # Too few events for reliable normal approximation - use more conservative approach
            # For very low counts, still calculate but be more conservative
            pass  # Continue with normal calculation but results should be interpreted carefully
        
        # Calculate proportions
        p1 = x1 / n1
        p2 = x2 / n2
        
        # Update metric values if not already set
        if metric.treatment_value is None:
            metric.treatment_value = p1
        if metric.control_value is None:
            metric.control_value = p2
        
        # Pooled proportion for standard error
        p_pooled = (x1 + x2) / (n1 + n2)
        
        # Standard error
        se = np.sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
        
        if se == 0:
            metric.p_value = 1.0
            metric.confidence_interval_lower = 0.0
            metric.confidence_interval_upper = 0.0
            return
        
        # Z-statistic and p-value (two-tailed test)
        z_stat = (p1 - p2) / se
        metric.p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))
        
        # 95% Confidence interval for difference in proportions
        se_diff = np.sqrt(p1*(1-p1)/n1 + p2*(1-p2)/n2)
        margin = 1.96 * se_diff
        metric.confidence_interval_lower = (p1 - p2) - margin
        metric.confidence_interval_upper = (p1 - p2) + margin
    
    def _calculate_continuous_statistics(self, metric: ExperimentMetric):
        """Two-sample t-test for continuous metrics"""
        
        # Ensure we have required data
        if not all([metric.treatment_value, metric.control_value,
                   metric.treatment_std, metric.control_std,
                   metric.treatment_sample_size, metric.control_sample_size]):
            return
        
        # Convert to numeric types (handle decimal.Decimal from Snowflake)
        try:
            mean1 = float(metric.treatment_value)
            mean2 = float(metric.control_value)
            std1 = float(metric.treatment_std)
            std2 = float(metric.control_std)
            n1 = int(float(metric.treatment_sample_size))
            n2 = int(float(metric.control_sample_size))
        except (ValueError, TypeError) as e:
            print(f"Error converting continuous statistics to float: {e}")
            return
        
        # Standard error of the difference
        se = np.sqrt((std1**2 / n1) + (std2**2 / n2))
        
        if se == 0:
            metric.p_value = 1.0 if mean1 == mean2 else 0.0
            metric.confidence_interval_lower = mean1 - mean2
            metric.confidence_interval_upper = mean1 - mean2
            return
        
        # T-statistic
        t_stat = (mean1 - mean2) / se
        
        # Degrees of freedom (Welch's t-test)
        df = (std1**2/n1 + std2**2/n2)**2 / ((std1**2/n1)**2/(n1-1) + (std2**2/n2)**2/(n2-1))
        
        # P-value (two-tailed test)
        metric.p_value = 2 * (1 - stats.t.cdf(abs(t_stat), df))
        
        # 95% Confidence interval for difference in means
        margin = stats.t.ppf(0.975, df) * se
        metric.confidence_interval_lower = (mean1 - mean2) - margin
        metric.confidence_interval_upper = (mean1 - mean2) + margin
    
    def apply_statsig_classification(self, metric: ExperimentMetric):
        """Apply Curie-style statistical significance classification"""
        
        # Handle None or invalid p_value
        if metric.p_value is None:
            metric.statsig_string = "unknown"
            return
        
        # Convert p_value to float to handle decimal.Decimal from Snowflake
        try:
            p_val = float(metric.p_value)
        except (ValueError, TypeError):
            metric.statsig_string = "unknown"
            return
        
        # Get the desired direction for this metric
        desired_direction = self._get_metric_desired_direction(metric.template_name, metric.metric_name)
        
        # Calculate relative impact (treatment vs control)
        if metric.treatment_value is not None and metric.control_value is not None and metric.control_value != 0:
            # Convert to float to handle decimal.Decimal types from Snowflake
            treatment_val = float(metric.treatment_value)
            control_val = float(metric.control_value)
            relative_impact = (treatment_val - control_val) / control_val
        else:
            relative_impact = float(metric.absolute_difference) if metric.absolute_difference is not None else 0
        
        # Debug logging for troubleshooting (uncomment if needed)
        # print(f"DEBUG: {metric.template_name} | {metric.metric_name} | desired_direction: {desired_direction} | relative_impact: {relative_impact} | p_value: {p_val}")
        
        # Apply Curie logic based on p-value thresholds
        if p_val < 0.05:
            # Significant result - check if direction aligns with desired
            if self._is_positive_impact(desired_direction, relative_impact):
                metric.statsig_string = "significant positive"
            else:
                metric.statsig_string = "significant negative"
        elif p_val < 0.25:
            # Directional result - check if direction aligns with desired
            if self._is_positive_impact(desired_direction, relative_impact):
                metric.statsig_string = "directional positive"
            else:
                metric.statsig_string = "directional negative"
        else:
            # No significant effect
            metric.statsig_string = "flat"
    
    def _is_positive_impact(self, desired_direction: str, relative_impact: float) -> bool:
        """Determine if the impact is in the desired direction (positive)"""
        if desired_direction is None:
            # If we don't know the desired direction, assume increase is positive
            return relative_impact > 0
        
        if desired_direction.lower() == 'increase':
            return relative_impact > 0
        elif desired_direction.lower() == 'decrease':
            return relative_impact < 0
        else:
            # Default to increase if direction is unclear
            return relative_impact > 0
    
    def calculate_statistical_power(self, metric: ExperimentMetric, effect_size: float = None):
        """
        Calculate statistical power (post-hoc power analysis)
        
        Args:
            metric: ExperimentMetric object
            effect_size: Expected effect size (optional)
        """
        
        if metric.metric_type == 'rate':
            metric.statistical_power = self._calculate_power_rate(metric, effect_size)
        elif metric.metric_type == 'continuous':
            metric.statistical_power = self._calculate_power_continuous(metric, effect_size)
        else:
            metric.statistical_power = None
    
    def _calculate_power_rate(self, metric: ExperimentMetric, effect_size: float = None) -> float:
        """Calculate power for rate/proportion metrics using two-proportion z-test"""
        
        # Ensure we have required data
        if not all([metric.treatment_denominator, metric.control_denominator,
                   metric.treatment_value is not None, metric.control_value is not None]):
            return None
        
        try:
            # Handle decimal.Decimal types from Snowflake
            n1 = float(metric.treatment_denominator)
            n2 = float(metric.control_denominator)
            p1 = float(metric.treatment_value)
            p2 = float(metric.control_value)
            
            # Use observed effect size if not provided
            if effect_size is None:
                effect_size = abs(p1 - p2)
            else:
                effect_size = abs(effect_size)
            
            # For very small effect sizes, return low power
            if effect_size == 0:
                return 0.05  # Type I error rate when no true effect
            
            # Pooled proportion for variance calculation
            p_pooled = (p1 * n1 + p2 * n2) / (n1 + n2)
            
            # Standard error under null hypothesis
            se_null = np.sqrt(p_pooled * (1 - p_pooled) * (1/n1 + 1/n2))
            
            # Standard error under alternative hypothesis  
            se_alt = np.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
            
            if se_null == 0 or se_alt == 0:
                return None
            
            # Critical value for two-tailed test at alpha = 0.05
            alpha = 0.05
            z_critical = stats.norm.ppf(1 - alpha/2)
            
            # Non-centrality parameter
            z_beta = (effect_size - z_critical * se_null) / se_alt
            
            # Power = P(Z > z_beta) where Z ~ N(0,1)
            power = 1 - stats.norm.cdf(z_beta)
            
            # Ensure power is between 0 and 1
            return max(0.0, min(1.0, power))
            
        except (ValueError, TypeError, ZeroDivisionError):
            return None
    
    def _calculate_power_continuous(self, metric: ExperimentMetric, effect_size: float = None) -> float:
        """Calculate power for continuous metrics using two-sample t-test"""
        
        # Ensure we have required data
        if not all([metric.treatment_value is not None, metric.control_value is not None,
                   metric.treatment_sample_size, metric.control_sample_size]):
            return None
        
        try:
            # Handle decimal.Decimal types from Snowflake
            mean1 = float(metric.treatment_value)
            mean2 = float(metric.control_value)
            n1 = int(float(metric.treatment_sample_size))
            n2 = int(float(metric.control_sample_size))
            
            # Get standard deviations
            std1 = float(metric.treatment_std) if metric.treatment_std is not None else None
            std2 = float(metric.control_std) if metric.control_std is not None else None
            
            # Use observed effect size if not provided
            if effect_size is None:
                effect_size = abs(mean1 - mean2)
            else:
                effect_size = abs(effect_size)
            
            if effect_size == 0:
                return 0.05  # Type I error rate when no true effect
            
            # If we don't have std devs, estimate from the data
            if std1 is None or std2 is None:
                # Use pooled standard deviation estimate based on effect size
                # This is a rough approximation
                pooled_std = effect_size / 0.5  # Assume medium effect size (Cohen's d ≈ 0.5)
                std1 = std1 or pooled_std
                std2 = std2 or pooled_std
            
            # Pooled standard deviation
            pooled_var = ((n1 - 1) * std1**2 + (n2 - 1) * std2**2) / (n1 + n2 - 2)
            pooled_std = np.sqrt(pooled_var)
            
            if pooled_std == 0:
                return None
            
            # Standard error
            se = pooled_std * np.sqrt(1/n1 + 1/n2)
            
            # Degrees of freedom
            df = n1 + n2 - 2
            
            # T-statistic under alternative hypothesis
            t_stat = effect_size / se
            
            # Critical value for two-tailed test at alpha = 0.05
            alpha = 0.05
            t_critical = stats.t.ppf(1 - alpha/2, df)
            
            # Power calculation using non-central t-distribution
            # Power = P(|T| > t_critical | ncp = t_stat)
            power = 1 - stats.t.cdf(t_critical, df, loc=t_stat) + stats.t.cdf(-t_critical, df, loc=t_stat)
            
            # Ensure power is between 0 and 1
            return max(0.0, min(1.0, power))
            
        except (ValueError, TypeError, ZeroDivisionError):
            return None
