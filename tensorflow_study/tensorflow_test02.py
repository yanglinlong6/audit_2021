def my_func():
    # TensorFlow and tf.keras
    import tensorflow as tf

    # Helper libraries
    import numpy as np
    import matplotlib.pyplot as plt
    print(tf.__version__)
    fashion_mnist = tf.keras.datasets.fashion_mnist
    (train_images, train_labels), (test_images, test_labels) = fashion_mnist.load_data()
    class_names = ['T-shirt/top', 'Trouser', 'Pullover', 'Dress', 'Coat', 'Sandal', 'Shirt', 'Sneaker', 'Bag',
                   'Ankle boot']
    shape = train_images.shape
    print(shape)
    print(len(train_labels))
    print(train_labels)
    print(test_images.shape)
    print(test_labels)

    plt.figure()
    plt.imshow(train_images[0])
    plt.colorbar()
    plt.grid(False)
    plt.show()

    train_images = train_images / 255.0
    test_images = test_images / 255.0
    plt.figure(figsize=(10, 10))
    for i in range(25):
        plt.subplot(5, 5, i + 1)
        plt.xticks([])
        plt.yticks([])
        plt.grid(False)
        plt.imshow(train_images[i], cmap=plt.cm.binary)
        plt.xlabel(class_names[train_labels[i]])
    plt.show()

    # 新建模型
    model = tf.keras.Sequential([
        tf.keras.layers.Flatten(input_shape=(28, 28)),
        tf.keras.layers.Dense(128, activation='relu'),
        tf.keras.layers.Dense(10)
    ])
    # 配置模型参数
    model.compile(optimizer='adam',
                  loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                  metrics=['accuracy'])
    # 训练模型参数
    model.fit(train_images, train_labels, epochs=10)

    # 评估模型准确度
    test_loss, test_acc = model.evaluate(test_images, test_labels, verbose=2)
    print('\nTest accuracy:', test_acc)

    probability_model = tf.keras.Sequential([model, tf.keras.layers.Softmax()])
    predictions = probability_model.predict(test_images)
    print(predictions[0])
    np.argmax(predictions[0])

    def plot_image(i, predictions_array, true_label, img):
        true_label, img = true_label[i], img[i]
        plt.grid(False)
        plt.xticks([])
        plt.yticks([])

        plt.imshow(img, cmap=plt.cm.binary)

        predicted_label = np.argmax(predictions_array)
        if predicted_label == true_label:
            color = 'blue'
        else:
            color = 'red'

        plt.xlabel("{} {:2.0f}% ({})".format(class_names[predicted_label],
                                             100 * np.max(predictions_array),
                                             class_names[true_label]),
                   color=color)

    def plot_value_array(i, predictions_array, true_label):
        true_label = true_label[i]
        plt.grid(False)
        plt.xticks(range(10))
        plt.yticks([])
        thisplot = plt.bar(range(10), predictions_array, color="#777777")
        plt.ylim([0, 1])
        predicted_label = np.argmax(predictions_array)

        thisplot[predicted_label].set_color('red')
        thisplot[true_label].set_color('blue')

    i = 0
    plt.figure(figsize=(6, 3))
    plt.subplot(1, 2, 1)
    plot_image(i, predictions[i], test_labels, test_images)
    plt.subplot(1, 2, 2)
    plot_value_array(i, predictions[i], test_labels)
    plt.show()

    i = 12
    plt.figure(figsize=(6, 3))
    plt.subplot(1, 2, 1)
    plot_image(i, predictions[i], test_labels, test_images)
    plt.subplot(1, 2, 2)
    plot_value_array(i, predictions[i], test_labels)
    plt.show()

    # Plot the first X test images, their predicted labels, and the true labels.
    # Color correct predictions in blue and incorrect predictions in red.
    num_rows = 5
    num_cols = 3
    num_images = num_rows * num_cols
    plt.figure(figsize=(2 * 2 * num_cols, 2 * num_rows))
    for i in range(num_images):
        plt.subplot(num_rows, 2 * num_cols, 2 * i + 1)
        plot_image(i, predictions[i], test_labels, test_images)
        plt.subplot(num_rows, 2 * num_cols, 2 * i + 2)
        plot_value_array(i, predictions[i], test_labels)
    plt.tight_layout()
    plt.show()


def my_func_02():
    import tensorflow as tf
    from tensorflow import keras
    import numpy as np
    print(tf.__version__)
    imdb = keras.datasets.imdb
    (train_data, train_labels), (test_data, test_labels) = imdb.load_data(num_words=10000)
    print("Training entries: {}, labels: {}".format(len(train_data), len(train_labels)))
    print(train_data[0])
    print(len(train_data[0]), len(train_data[1]))

    # 一个映射单词到整数索引的词典
    word_index = imdb.get_word_index()

    # 保留第一个索引
    word_index = {k: (v + 3) for k, v in word_index.items()}
    word_index["<PAD>"] = 0
    word_index["<START>"] = 1
    word_index["<UNK>"] = 2  # unknown
    word_index["<UNUSED>"] = 3

    reverse_word_index = dict([(value, key) for (key, value) in word_index.items()])

    def decode_review(text):
        return ' '.join([reverse_word_index.get(i, '?') for i in text])

    print('decode_review', decode_review(train_data[0]))

    train_data = keras.preprocessing.sequence.pad_sequences(train_data,
                                                            value=word_index["<PAD>"],
                                                            padding='post',
                                                            maxlen=256)

    test_data = keras.preprocessing.sequence.pad_sequences(test_data,
                                                           value=word_index["<PAD>"],
                                                           padding='post',
                                                           maxlen=256)
    # 输入形状是用于电影评论的词汇数目（10,000 词）
    vocab_size = 10000

    model = keras.Sequential()
    model.add(keras.layers.Embedding(vocab_size, 16))
    model.add(keras.layers.GlobalAveragePooling1D())
    model.add(keras.layers.Dense(16, activation='relu'))
    model.add(keras.layers.Dense(1, activation='sigmoid'))

    print(model.summary())
    model.compile(optimizer='adam',
                  loss='binary_crossentropy',
                  metrics=['accuracy'])
    x_val = train_data[:10000]
    partial_x_train = train_data[10000:]

    y_val = train_labels[:10000]
    partial_y_train = train_labels[10000:]

    history = model.fit(partial_x_train,
                        partial_y_train,
                        epochs=40,
                        batch_size=512,
                        validation_data=(x_val, y_val),
                        verbose=1)

    results = model.evaluate(test_data, test_labels, verbose=2)
    print(results)
    history_dict = history.history
    print(history_dict.keys())

    import matplotlib.pyplot as plt

    acc = history_dict['accuracy']
    val_acc = history_dict['val_accuracy']
    loss = history_dict['loss']
    val_loss = history_dict['val_loss']

    epochs = range(1, len(acc) + 1)

    # “bo”代表 "蓝点"
    plt.plot(epochs, loss, 'bo', label='Training loss')
    # b代表“蓝色实线”
    plt.plot(epochs, val_loss, 'b', label='Validation loss')
    plt.title('Training and validation loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.legend()
    plt.show()

    plt.clf()  # 清除数字
    plt.plot(epochs, acc, 'bo', label='Training acc')
    plt.plot(epochs, val_acc, 'b', label='Validation acc')
    plt.title('Training and validation accuracy')
    plt.xlabel('Epochs')
    plt.ylabel('Accuracy')
    plt.legend()

    plt.show()


def my_func_03():
    import os
    import numpy as np

    import tensorflow as tf
    import tensorflow_hub as hub
    import tensorflow_datasets as tfds

    print("Version: ", tf.__version__)
    print("Eager mode: ", tf.executing_eagerly())
    print("Hub version: ", hub.__version__)
    print("GPU is", "available" if tf.config.list_physical_devices("GPU") else "NOT AVAILABLE")

    train_data, validation_data, test_data = tfds.load(
        name="imdb_reviews",
        split=('train[:60%]', 'train[60%:]', 'test'),
        as_supervised=True)

    train_examples_batch, train_labels_batch = next(iter(train_data.batch(10)))
    print(train_examples_batch)
    print(train_labels_batch)
    embedding = "https://tfhub.dev/google/nnlm-en-dim50/2"
    hub_layer = hub.KerasLayer(embedding, input_shape=[],
                               dtype=tf.string, trainable=True)
    hub_layer(train_examples_batch[:3])
    model = tf.keras.Sequential()
    model.add(hub_layer)
    model.add(tf.keras.layers.Dense(16, activation='relu'))
    model.add(tf.keras.layers.Dense(1))

    print(model.summary())
    model.compile(optimizer='adam',
                  loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
                  metrics=['accuracy'])
    history = model.fit(train_data.shuffle(10000).batch(512),
                        epochs=10,
                        validation_data=validation_data.batch(512),
                        verbose=1)
    results = model.evaluate(test_data.batch(512), verbose=2)

    for name, value in zip(model.metrics_names, results):
        print("%s: %.3f" % (name, value))


def my_func_04():
    import pathlib
    import matplotlib.pyplot as plt
    import pandas as pd
    import seaborn as sns

    import tensorflow as tf

    from tensorflow import keras
    from tensorflow.python.keras import layers

    print(tf.__version__)
    dataset_path = keras.utils.get_file("auto-mpg.data",
                                        "http://archive.ics.uci.edu/ml/machine-learning-databases/auto-mpg/auto-mpg.data")
    print(dataset_path)
    column_names = ['MPG', 'Cylinders', 'Displacement', 'Horsepower', 'Weight',
                    'Acceleration', 'Model Year', 'Origin']
    raw_dataset = pd.read_csv(dataset_path, names=column_names,
                              na_values="?", comment='\t',
                              sep=" ", skipinitialspace=True)

    dataset = raw_dataset.copy()
    print(dataset.tail())
    print(dataset.isna().sum())
    dataset = dataset.dropna()
    origin = dataset.pop('Origin')
    dataset['USA'] = (origin == 1) * 1.0
    dataset['Europe'] = (origin == 2) * 1.0
    dataset['Japan'] = (origin == 3) * 1.0
    print(dataset.tail())
    train_dataset = dataset.sample(frac=0.8, random_state=0)
    test_dataset = dataset.drop(train_dataset.index)
    sns.pairplot(train_dataset[["MPG", "Cylinders", "Displacement", "Weight"]], diag_kind="kde")
    train_stats = train_dataset.describe()
    train_stats.pop("MPG")
    train_stats = train_stats.transpose()
    print(train_stats)
    train_labels = train_dataset.pop('MPG')
    test_labels = test_dataset.pop('MPG')

    def norm(x):
        return (x - train_stats['mean']) / train_stats['std']

    normed_train_data = norm(train_dataset)
    normed_test_data = norm(test_dataset)

    def build_model():
        model = keras.Sequential([
            layers.Dense(64, activation='relu', input_shape=[len(train_dataset.keys())]),
            layers.Dense(64, activation='relu'),
            layers.Dense(1)
        ])

        optimizer = tf.keras.optimizers.RMSprop(0.001)
        model.compile(loss='mse',
                      optimizer=optimizer,
                      metrics=['mae', 'mse'])
        return model

    model = build_model()

    # 通过为每个完成的时期打印一个点来显示训练进度
    class PrintDot(keras.callbacks.Callback):
        def on_epoch_end(self, epoch, logs):
            if epoch % 100 == 0: print('')
            print('.', end='')

    EPOCHS = 1000

    history = model.fit(
        normed_train_data, train_labels,
        epochs=EPOCHS, validation_split=0.2, verbose=0,
        callbacks=[PrintDot()])

    hist = pd.DataFrame(history.history)
    hist['epoch'] = history.epoch
    hist.tail()

    def plot_history(history):
        hist = pd.DataFrame(history.history)
        hist['epoch'] = history.epoch

        plt.figure()
        plt.xlabel('Epoch')
        plt.ylabel('Mean Abs Error [MPG]')
        plt.plot(hist['epoch'], hist['mae'],
                 label='Train Error')
        plt.plot(hist['epoch'], hist['val_mae'],
                 label='Val Error')
        plt.ylim([0, 5])
        plt.legend()

        plt.figure()
        plt.xlabel('Epoch')
        plt.ylabel('Mean Square Error [$MPG^2$]')
        plt.plot(hist['epoch'], hist['mse'],
                 label='Train Error')
        plt.plot(hist['epoch'], hist['val_mse'],
                 label='Val Error')
        plt.ylim([0, 20])
        plt.legend()
        plt.show()

    plot_history(history)

    model = build_model()

    # patience 值用来检查改进 epochs 的数量
    early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=10)

    history = model.fit(normed_train_data, train_labels, epochs=EPOCHS,
                        validation_split=0.2, verbose=0, callbacks=[early_stop, PrintDot()])

    plot_history(history)

    loss, mae, mse = model.evaluate(normed_test_data, test_labels, verbose=2)
    print("Testing set Mean Abs Error: {:5.2f} MPG".format(mae))

    test_predictions = model.predict(normed_test_data).flatten()
    plt.scatter(test_labels, test_predictions)
    plt.xlabel('True Values [MPG]')
    plt.ylabel('Predictions [MPG]')
    plt.axis('equal')
    plt.axis('square')
    plt.xlim([0, plt.xlim()[1]])
    plt.ylim([0, plt.ylim()[1]])
    _ = plt.plot([-100, 100], [-100, 100])
    error = test_predictions - test_labels
    plt.hist(error, bins=25)
    plt.xlabel("Prediction Error [MPG]")
    _ = plt.ylabel("Count")


def my_func_05():
    import tensorflow as tf
    from tensorflow.keras import layers
    from tensorflow.keras import regularizers

    print(tf.__version__)
    import tensorflow_docs as tfdocs
    import tensorflow_docs.modeling
    import tensorflow_docs.plots
    from IPython import display
    from matplotlib import pyplot as plt

    import numpy as np

    import pathlib
    import shutil
    import tempfile

    logdir = pathlib.Path(tempfile.mkdtemp()) / "tensorboard_logs"
    shutil.rmtree(logdir, ignore_errors=True)
    gz = tf.keras.utils.get_file('HIGGS.csv.gz', 'http://mlphysics.ics.uci.edu/data/higgs/HIGGS.csv.gz')
    FEATURES = 28
    ds = tf.data.experimental.CsvDataset(gz, [float(), ] * (FEATURES + 1), compression_type="GZIP")

    def pack_row(*row):
        label = row[0]
        features = tf.stack(row[1:], 1)
        return features, label

    packed_ds = ds.batch(10000).map(pack_row).unbatch()
    for features, label in packed_ds.batch(1000).take(1):
        print(features[0])
        plt.hist(features.numpy().flatten(), bins=101)

    N_VALIDATION = int(1e3)
    N_TRAIN = int(1e4)
    BUFFER_SIZE = int(1e4)
    BATCH_SIZE = 500
    STEPS_PER_EPOCH = N_TRAIN // BATCH_SIZE
    validate_ds = packed_ds.take(N_VALIDATION).cache()
    train_ds = packed_ds.skip(N_VALIDATION).take(N_TRAIN).cache()
    validate_ds = validate_ds.batch(BATCH_SIZE)
    train_ds = train_ds.shuffle(BUFFER_SIZE).repeat().batch(BATCH_SIZE)
    lr_schedule = tf.keras.optimizers.schedules.InverseTimeDecay(
        0.001,
        decay_steps=STEPS_PER_EPOCH * 1000,
        decay_rate=1,
        staircase=False)

    def get_optimizer():
        return tf.keras.optimizers.Adam(lr_schedule)

    step = np.linspace(0, 100000)
    lr = lr_schedule(step)
    plt.figure(figsize=(8, 6))
    plt.plot(step / STEPS_PER_EPOCH, lr)
    plt.ylim([0, max(plt.ylim())])
    plt.xlabel('Epoch')
    _ = plt.ylabel('Learning Rate')

    def get_callbacks(name):
        return [
            tfdocs.modeling.EpochDots(),
            tf.keras.callbacks.EarlyStopping(monitor='val_binary_crossentropy', patience=200),
            tf.keras.callbacks.TensorBoard(logdir / name),
        ]

    def compile_and_fit(model, name, optimizer=None, max_epochs=10000):
        if optimizer is None:
            optimizer = get_optimizer()
        model.compile(optimizer=optimizer,
                      loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
                      metrics=[
                          tf.keras.losses.BinaryCrossentropy(
                              from_logits=True, name='binary_crossentropy'),
                          'accuracy'])

        model.summary()

        history = model.fit(
            train_ds,
            steps_per_epoch=STEPS_PER_EPOCH,
            epochs=max_epochs,
            validation_data=validate_ds,
            callbacks=get_callbacks(name),
            verbose=0)
        return history

    tiny_model = tf.keras.Sequential([
        layers.Dense(16, activation='elu', input_shape=(FEATURES,)),
        layers.Dense(1)
    ])
    size_histories = {}
    size_histories['Tiny'] = compile_and_fit(tiny_model, 'sizes/Tiny')
    plotter = tfdocs.plots.HistoryPlotter(metric='binary_crossentropy', smoothing_std=10)
    plotter.plot(size_histories)
    plt.ylim([0.5, 0.7])
    small_model = tf.keras.Sequential([
        # `input_shape` is only required here so that `.summary` works.
        layers.Dense(16, activation='elu', input_shape=(FEATURES,)),
        layers.Dense(16, activation='elu'),
        layers.Dense(1)
    ])
    size_histories['Small'] = compile_and_fit(small_model, 'sizes/Small')
    medium_model = tf.keras.Sequential([
        layers.Dense(64, activation='elu', input_shape=(FEATURES,)),
        layers.Dense(64, activation='elu'),
        layers.Dense(64, activation='elu'),
        layers.Dense(1)
    ])
    size_histories['Medium'] = compile_and_fit(medium_model, "sizes/Medium")
    large_model = tf.keras.Sequential([
        layers.Dense(512, activation='elu', input_shape=(FEATURES,)),
        layers.Dense(512, activation='elu'),
        layers.Dense(512, activation='elu'),
        layers.Dense(512, activation='elu'),
        layers.Dense(1)
    ])
    size_histories['large'] = compile_and_fit(large_model, "sizes/large")
    plotter.plot(size_histories)
    a = plt.xscale('log')
    plt.xlim([5, max(plt.xlim())])
    plt.ylim([0.5, 0.7])
    plt.xlabel("Epochs [Log Scale]")

    shutil.rmtree(logdir / 'regularizers/Tiny', ignore_errors=True)
    shutil.copytree(logdir / 'sizes/Tiny', logdir / 'regularizers/Tiny')
    regularizer_histories = {}
    regularizer_histories['Tiny'] = size_histories['Tiny']
    l2_model = tf.keras.Sequential([
        layers.Dense(512, activation='elu',
                     kernel_regularizer=regularizers.l2(0.001),
                     input_shape=(FEATURES,)),
        layers.Dense(512, activation='elu',
                     kernel_regularizer=regularizers.l2(0.001)),
        layers.Dense(512, activation='elu',
                     kernel_regularizer=regularizers.l2(0.001)),
        layers.Dense(512, activation='elu',
                     kernel_regularizer=regularizers.l2(0.001)),
        layers.Dense(1)
    ])

    regularizer_histories['l2'] = compile_and_fit(l2_model, "regularizers/l2")
    plotter.plot(regularizer_histories)
    plt.ylim([0.5, 0.7])
    result = l2_model(features)
    regularization_loss = tf.add_n(l2_model.losses)

    dropout_model = tf.keras.Sequential([
        layers.Dense(512, activation='elu', input_shape=(FEATURES,)),
        layers.Dropout(0.5),
        layers.Dense(512, activation='elu'),
        layers.Dropout(0.5),
        layers.Dense(512, activation='elu'),
        layers.Dropout(0.5),
        layers.Dense(512, activation='elu'),
        layers.Dropout(0.5),
        layers.Dense(1)
    ])

    regularizer_histories['dropout'] = compile_and_fit(dropout_model, "regularizers/dropout")
    plotter.plot(regularizer_histories)
    plt.ylim([0.5, 0.7])

    combined_model = tf.keras.Sequential([
        layers.Dense(512, kernel_regularizer=regularizers.l2(0.0001),
                     activation='elu', input_shape=(FEATURES,)),
        layers.Dropout(0.5),
        layers.Dense(512, kernel_regularizer=regularizers.l2(0.0001),
                     activation='elu'),
        layers.Dropout(0.5),
        layers.Dense(512, kernel_regularizer=regularizers.l2(0.0001),
                     activation='elu'),
        layers.Dropout(0.5),
        layers.Dense(512, kernel_regularizer=regularizers.l2(0.0001),
                     activation='elu'),
        layers.Dropout(0.5),
        layers.Dense(1)
    ])

    regularizer_histories['combined'] = compile_and_fit(combined_model, "regularizers/combined")
    plotter.plot(regularizer_histories)
    plt.ylim([0.5, 0.7])


def my_func_06():
    import os

    import tensorflow as tf
    from tensorflow import keras

    print(tf.version.VERSION)

    (train_images, train_labels), (test_images, test_labels) = tf.keras.datasets.mnist.load_data()

    train_labels = train_labels[:1000]
    test_labels = test_labels[:1000]

    train_images = train_images[:1000].reshape(-1, 28 * 28) / 255.0
    test_images = test_images[:1000].reshape(-1, 28 * 28) / 255.0

    def create_model():
        model = tf.keras.Sequential([
            keras.layers.Dense(512, activation='relu', input_shape=(784,)),
            keras.layers.Dropout(0.2),
            keras.layers.Dense(10)
        ])

        model.compile(optimizer='adam',
                      loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                      metrics=[tf.keras.metrics.SparseCategoricalAccuracy()])

        return model

    # Create a basic model instance
    model = create_model()

    # Display the model's architecture
    print(model.summary())

    checkpoint_path = "training_1/cp.ckpt"
    checkpoint_dir = os.path.dirname(checkpoint_path)

    cp_callback = tf.keras.callbacks.ModelCheckpoint(filepath=checkpoint_path,
                                                     save_weights_only=True,
                                                     verbose=1)

    model.fit(train_images,
              train_labels,
              epochs=10,
              validation_data=(test_images, test_labels),
              callbacks=[cp_callback])
    print(os.listdir(checkpoint_dir))

    # Create a basic model instance
    model = create_model()

    # Evaluate the model
    loss, acc = model.evaluate(test_images, test_labels, verbose=2)
    print("Untrained model, accuracy: {:5.2f}%".format(100 * acc))

    # Loads the weights
    model.load_weights(checkpoint_path)

    # Re-evaluate the model
    loss, acc = model.evaluate(test_images, test_labels, verbose=2)
    print("Restored model, accuracy: {:5.2f}%".format(100 * acc))

    # Include the epoch in the file name (uses `str.format`)
    checkpoint_path = "training_2/cp-{epoch:04d}.ckpt"
    checkpoint_dir = os.path.dirname(checkpoint_path)

    batch_size = 32

    # Create a callback that saves the model's weights every 5 epochs
    cp_callback = tf.keras.callbacks.ModelCheckpoint(
        filepath=checkpoint_path,
        verbose=1,
        save_weights_only=True,
        save_freq=5 * batch_size)

    # Create a new model instance
    model = create_model()

    # Save the weights using the `checkpoint_path` format
    model.save_weights(checkpoint_path.format(epoch=0))

    # Train the model with the new callback
    model.fit(train_images,
              train_labels,
              epochs=50,
              batch_size=batch_size,
              callbacks=[cp_callback],
              validation_data=(test_images, test_labels),
              verbose=0)

    print(os.listdir(checkpoint_dir))

    latest = tf.train.latest_checkpoint(checkpoint_dir)
    print(latest)

    # Create a new model instance
    model = create_model()

    # Load the previously saved weights
    model.load_weights(latest)

    # Re-evaluate the model
    loss, acc = model.evaluate(test_images, test_labels, verbose=2)
    print("Restored model, accuracy: {:5.2f}%".format(100 * acc))

    # Save the weights
    model.save_weights('./checkpoints/my_checkpoint')

    # Create a new model instance
    model = create_model()

    # Restore the weights
    model.load_weights('./checkpoints/my_checkpoint')

    # Evaluate the model
    loss, acc = model.evaluate(test_images, test_labels, verbose=2)
    print("Restored model, accuracy: {:5.2f}%".format(100 * acc))

    # Create and train a new model instance.
    model = create_model()
    model.fit(train_images, train_labels, epochs=5)

    # Save the entire model as a SavedModel.
    model.save('saved_model/my_model')


if __name__ == '__main__':
    # pass
    # my_func()
    # my_func_02()
    # my_func_03()
    # my_func_04()
    my_func_05()
