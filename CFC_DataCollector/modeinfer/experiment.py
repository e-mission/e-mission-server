import scipy.io
import random

mat = scipy.io.loadmat('original_data.mat')
#modified_mat = scipy.io.loadmat('modified_data.mat')

rows = mat['X'].shape[0]
test_size = 50

orig_training_set = mat['X'][0:rows-test_size]
orig_training_labels = mat['y'][0:rows-test_size]

orig_test_set = mat['X'][rows-test_size:]
orig_test_labels = mat['y'][rows-test_size:]

#mod_training_set = modified_mat['X'][0:rows-test_size]
#mod_training_labels = modified_mat['y'][0:rows-test_size]

#mod_test_set = modified_mat['X'][rows-test_size:]
#mod_test_labels = modified_mat['y'][rows-test_size:]


def buildModelStep(selFeatureMatrix, cleanedResultVector):
	from sklearn import ensemble
	forestClf = ensemble.RandomForestClassifier()
	model = forestClf.fit(selFeatureMatrix, cleanedResultVector)
	return model
	
model = buildModelStep(orig_training_set, orig_training_labels)
#new_model = buildModelStep(mat['X'], mat['y'].ravel())

predicted_labels = model.predict(orig_test_set)
print(predicted_labels)
print(orig_test_labels)

def score_classifier(predicted_labels, true_labels):
	correct = 0
	wrong = 0
	c = 0
	while c < len(predicted_labels):
		if predicted_labels[c] == true_labels[c]:
			correct += 1
		else:
			wrong += 1
		c += 1
	return (correct, wrong)

correct, wrong = score_classifier(predicted_labels, orig_test_labels)
print("Your classifier got " + str(correct) + " correct labels.")
print("Your classifier got " + str(wrong) + " wrong labels.")
print("Total accuracy: " + str((float(correct)/float(correct+wrong))*100.0))

	