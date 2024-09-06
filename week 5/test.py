import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix

__author__ = 'Darshan Dhanani'

# Example confusion matrix (replace with your actual confusion matrix)
conf_matrix = [[100, 5, 10],
               [3, 150, 7],
               [8, 6, 120]]

# Labels for classes
class_labels = ['A', 'B', 'C']

# Plot confusion matrix as heatmap
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, cmap='Blues', fmt='g', xticklabels=class_labels, yticklabels=class_labels)
plt.xlabel('Predicted Value')
plt.ylabel('Actual Value')
plt.title('Confusion Matrix')
plt.show()
