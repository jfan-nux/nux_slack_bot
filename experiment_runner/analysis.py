"""
Statistical Analysis - Calculate p-values, confidence intervals, and significance classification
"""

import numpy as np
from scipy import stats
from .results_parser import ExperimentMetric

class ExperimentAnalysis:
    """Statistical analysis for experiment metrics"""
    
    def calculate_statistics(self, metric: ExperimentMetric):
        """Calculate p-value, confidence intervals, and statistical power"""
        
        if metric.metric_type == 'rate':
            self._calculate_rate_statistics(metric)
        elif metric.metric_type == 'continuous':
            self._calculate_continuous_statistics(metric)
        
        # Calculate absolute difference regardless of metric type
        if metric.treatment_value is not None and metric.control_value is not None:
            metric.absolute_difference = metric.treatment_value - metric.control_value
    
    def _calculate_rate_statistics(self, metric: ExperimentMetric):
        """Two-proportion z-test for rate metrics"""
        
        # Ensure we have required data
        if not all([metric.treatment_numerator is not None, metric.treatment_denominator,
                   metric.control_numerator is not None, metric.control_denominator]):
            return
        
        # Convert to numeric types
        x1 = float(metric.treatment_numerator)  # treatment successes
        n1 = float(metric.treatment_denominator)  # treatment total
        x2 = float(metric.control_numerator)  # control successes
        n2 = float(metric.control_denominator)  # control total
        
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
        
        # Convert to numeric types
        mean1 = float(metric.treatment_value)
        mean2 = float(metric.control_value)
        std1 = float(metric.treatment_std)
        std2 = float(metric.control_std)
        n1 = int(metric.treatment_sample_size)
        n2 = int(metric.control_sample_size)
        
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
        
        if metric.p_value is None:
            metric.statsig_string = "insufficient_data"
            return
        
        p_val = metric.p_value
        
        if p_val <= 0.01:
            metric.statsig_string = "highly_significant"
        elif p_val <= 0.05:
            metric.statsig_string = "significant"
        elif p_val <= 0.10:
            metric.statsig_string = "trending"
        else:
            metric.statsig_string = "not_significant"
    
    def calculate_statistical_power(self, metric: ExperimentMetric, effect_size: float = None):
        """
        Calculate statistical power (post-hoc power analysis)
        
        Args:
            metric: ExperimentMetric object
            effect_size: Expected effect size (optional)
        """
        
        # This is a placeholder for more sophisticated power analysis
        # Implementation would depend on specific requirements and effect size assumptions
        metric.statistical_power = None
